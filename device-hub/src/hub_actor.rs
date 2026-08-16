use super::protocol::Color;
use serde::Serialize;
use serde::Serializer;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// Main Actor that contain the hub global state
/// Receives messages via `mpsc` channel & responds via `oneshot` channel
pub struct HubData {
    receiver: mpsc::Receiver<Query>,
    state: StateSnapshot,
}

/// Messages accepted by the actor [`HubData`].
enum Query {
    GetSnapshot {
        respond_to: oneshot::Sender<StateSnapshot>,
    },
    SetLight {
        light: LightColor,
        respond_to: oneshot::Sender<()>,
    },
    SetSensor {
        sensor: Sensor,
        respond_to: oneshot::Sender<()>,
    },

    SetButton {
        button: Button,
        respond_to: oneshot::Sender<()>,
    },
}

pub struct Button {
    pub button_id: u8,
    pub is_pressed: bool,
}

pub struct Sensor {
    pub sensor_id: u8,
    pub value: u16,
}

pub struct LightColor {
    pub light_id: u8,
    pub color: Color,
}

/// Snapshot immutable of the global state.
#[derive(Clone, Default, Serialize)]
pub struct StateSnapshot {
    pub lights: HashMap<u8, Color>,
    pub buttons: HashMap<u8, ButtonState>,
    pub sensors: HashMap<u8, u16>,
}

#[derive(Clone, Copy, Default)]
pub struct ButtonState(pub bool);

impl Serialize for ButtonState {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(if self.0 { "pressed" } else { "released" })
    }
}

impl HubData {
    fn new(receiver: mpsc::Receiver<Query>) -> Self {
        HubData {
            receiver,
            state: StateSnapshot::default(),
        }
    }

    fn handle_message(&mut self, msg: Query) {
        match msg {
            Query::GetSnapshot { respond_to } => {
                let new_state = self.state.clone();
                _ = respond_to.send(new_state);
            }
            Query::SetButton { button, respond_to } => {
                self.state
                    .buttons
                    .insert(button.button_id, ButtonState(button.is_pressed));
                let _ = respond_to.send(());
            }
            Query::SetLight { light, respond_to } => {
                self.state.lights.insert(light.light_id, light.color);
                let _ = respond_to.send(());
            }
            Query::SetSensor { sensor, respond_to } => {
                self.state.sensors.insert(sensor.sensor_id, sensor.value);
                let _ = respond_to.send(());
            }
        }
    }
}

/// Main loop of the actor: processes incoming messages until the channel is closed
async fn run_hub(mut hub_data: HubData) {
    while let Some(msg) = hub_data.receiver.recv().await {
        hub_data.handle_message(msg);
    }
}

/// Serializes a [`StateSnapshot`] to raw JSON (bytes).
pub fn build_state_json(snapshot: &StateSnapshot) -> Vec<u8> {
    serde_json::to_vec(snapshot).expect("serialization cannot fail")
}

/// Cloneable sender for communicating with the [`HubData`] actor.
#[derive(Clone)]
pub struct HubHandle {
    sender: mpsc::Sender<Query>,
}

impl HubHandle {
    /// Creates a new [`HubData`] actor & returns a sender
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let hub_receiver = HubData::new(receiver);
        tokio::spawn(run_hub(hub_receiver));

        Self { sender }
    }

    /// Requests the current state snapshot from the actor
    pub async fn get_snapshot(&self) -> StateSnapshot {
        let (send, recv) = oneshot::channel();
        let msg = Query::GetSnapshot { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Monitor task has been killed")
    }

    /// Updates the color of a light in the global state
    pub async fn set_light(&self, light: LightColor) {
        let (send, recv) = oneshot::channel();
        let msg = Query::SetLight {
            light,
            respond_to: (send),
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Monitor task has been killed");
    }

    /// Updates the state of a button in the global state
    pub async fn set_button(&self, button: Button) {
        let (send, recv) = oneshot::channel();
        let msg = Query::SetButton {
            button,
            respond_to: (send),
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Monitor task has been killed");
    }

    /// Updates the value of a sensor in the global state
    pub async fn set_sensor(&self, sensor: Sensor) {
        let (send, recv) = oneshot::channel();
        let msg = Query::SetSensor {
            sensor,
            respond_to: (send),
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Monitor task has been killed");
    }
}
