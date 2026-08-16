use super::connection::Device;
use super::hub_actor;
use super::protocol::*;
use crate::error::HubError;
use crate::hub_actor::{Button, HubHandle, LightColor, Sensor};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::broadcast;
use tracing::{debug, info, instrument, warn};

/// Routes the device to the appropriate handler
pub async fn hub_dispatch(
    device: Device,
    hub: &HubHandle,
    light_tx: &broadcast::Sender<Vec<u8>>,
) -> Result<(), HubError> {
    info!(conn_id = device.connexion_id, dev_type = ?device.dev_type, "device connected");
    match device.dev_type {
        DeviceType::Controller => {
            handle_controller(device, hub, light_tx).await?;
        }
        DeviceType::Light => {
            handle_light(device, light_tx).await?;
        }
        DeviceType::Sensor => {
            handle_sensor(device, hub).await?;
        }
        DeviceType::Monitor => {
            handle_monitor(device, hub).await?;
        }
    }
    Ok(())
}

/// Handles a Controller connection: reads button events, updates the state,
/// sends light commands via broadcast, and returns LED feedback to the controller
#[instrument(skip(hub, light_tx), fields(conn_id = device.connexion_id))]
async fn handle_controller(
    device: Device,
    hub: &HubHandle,
    light_tx: &broadcast::Sender<Vec<u8>>,
) -> Result<(), HubError> {
    let mut stream = device.stream;

    loop {
        let mut tag = [0u8; 1];
        match stream.read_exact(&mut tag).await {
            Err(e) => {
                debug!("controller disconnected: {e}");
                break;
            }
            Ok(_) if tag[0] != TAG_BUTTON_EVENT => {
                warn!(tag = tag[0], "unexpected tag, ignoring");
                continue;
            }
            _ => {}
        }

        let mut buf = [0u8; 2];
        if stream.read_exact(&mut buf).await.is_err() {
            break;
        }

        let button_id = buf[0];
        let is_pressed = buf[1] == 0x01;
        debug!(button_id, is_pressed, "button event");

        hub.set_button(Button {
            button_id,
            is_pressed,
        })
        .await;

        if is_pressed {
            let color = match button_id {
                0 => Some(Color::RED),
                1 => Some(Color::GREEN),
                2 => Some(Color::BLUE),
                _ => None,
            };
            if let Some(c) = color {
                debug!(
                    button_id,
                    r = c.r,
                    g = c.g,
                    b = c.b,
                    "broadcasting light command"
                );
                hub.set_light(LightColor {
                    light_id: 0,
                    color: c,
                })
                .await;

                let _ = light_tx.send(vec![TAG_LIGHT_CMD, c.r, c.g, c.b]);
            }
        }

        let (r, g, b) = if is_pressed {
            (255, 255, 255)
        } else {
            (0, 0, 0)
        };
        if stream
            .write_all(&[TAG_LED_FEEDBACK, button_id, r, g, b])
            .await
            .is_err()
        {
            debug!("controller write failed, disconnecting");
            break;
        }
        let _ = stream.flush().await;
    }
    info!("controller disconnected");
    Ok(())
}

/// Handles a Light connection: subscribes to the light command broadcast
/// and forwards them to the device over TCP
#[instrument(skip(light_tx), fields(conn_id = device.connexion_id, light_id = device.dev_id))]
async fn handle_light(
    device: Device,
    light_tx: &broadcast::Sender<Vec<u8>>,
) -> Result<(), HubError> {
    let (_, mut writer) = device.stream.into_split();
    let mut rx = light_tx.subscribe();
    info!("light connected");

    while let Ok(data) = rx.recv().await {
        debug!(bytes = data.len(), "sending light command");
        if writer.write_all(&data).await.is_err() {
            debug!("light write failed, disconnecting");
            break;
        }
        let _ = writer.flush().await;
    }
    info!("light disconnected");
    Ok(())
}

/// Handles a Sensor connection: reads sensor values in a loop
/// and sends them to the HubData actor
#[instrument(skip(sensor_sender), fields(conn_id = device.connexion_id, sensor_id = device.dev_id))]
async fn handle_sensor(device: Device, sensor_sender: &HubHandle) -> Result<(), HubError> {
    let (reader, _) = device.stream.into_split();
    let mut reader = reader;
    info!("sensor connected");
    loop {
        let mut tag = [0u8; 1];
        match reader.read_exact(&mut tag).await {
            Ok(_) => {}
            Err(e) => {
                debug!("sensor disconnected: {e}");
                break;
            }
        }

        if tag[0] != TAG_SENSOR_VALUE {
            warn!(tag = tag[0], "unexpected sensor tag, ignoring");
            continue;
        }

        let mut buf = [0u8; 2];
        match reader.read_exact(&mut buf).await {
            Ok(_) => {}
            Err(_) => break,
        }

        let value = u16::from_be_bytes(buf);
        debug!(sensor_id = device.dev_id, value, "sensor value received");
        sensor_sender
            .set_sensor(Sensor {
                sensor_id: device.dev_id,
                value: value,
            })
            .await;
    }
    info!("sensor disconnected");
    Ok(())
}

/// Handles a Monitor connection: responds to state queries by serializing
/// and sending a JSON snapshot of the global state
#[instrument(skip(monitor_sender), fields(conn_id = device.connexion_id))]
async fn handle_monitor(device: Device, monitor_sender: &HubHandle) -> Result<(), HubError> {
    let (reader, writer) = device.stream.into_split();
    let mut reader = reader;
    let mut writer = writer;
    info!("monitor connected");
    loop {
        let mut tag = [0u8; 1];
        match reader.read_exact(&mut tag).await {
            Err(e) => {
                debug!("monitor disconnected: {e}");
                break;
            }
            Ok(_) if tag[0] != TAG_STATE_QUERY => {
                warn!(tag = tag[0], "unexpected monitor tag");
                break;
            }
            _ => {}
        }
        debug!("state query received");
        let snapshot = monitor_sender.get_snapshot().await;
        let json_snapshot = hub_actor::build_state_json(&snapshot);
        let len = (json_snapshot.len() as u32).to_be_bytes();

        let _ = writer.write_all(&len).await;
        let _ = writer.write_all(&json_snapshot).await;
        let _ = writer.flush().await;
        debug!(bytes = json_snapshot.len(), "state snapshot sent");
    }
    info!("monitor disconnected");
    Ok(())
}
