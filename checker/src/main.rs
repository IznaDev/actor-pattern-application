use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

const HOST: &str = "127.0.0.1";
const PORT: u16 = 8089;
const TIMEOUT: Duration = Duration::from_secs(2);

const HANDSHAKE_MAGIC: u8 = 0xA0;
const HANDSHAKE_ACK: u8 = 0xAC;

const DEVICE_CONTROLLER: u8 = 0x01;
const DEVICE_LIGHT: u8 = 0x02;
const DEVICE_SENSOR: u8 = 0x03;
const DEVICE_MONITOR: u8 = 0xFF;

const TAG_BUTTON_EVENT: u8 = 0x01;
const TAG_LED_FEEDBACK: u8 = 0x02;
const TAG_LIGHT_CMD: u8 = 0x03;
const TAG_SENSOR_VALUE: u8 = 0x04;
const TAG_STATE_QUERY: u8 = 0x10;

#[derive(Debug, Clone)]
enum ControllerCommand {
    ButtonPress(u8),
    ButtonRelease(u8),
    Shutdown,
}

#[derive(Debug, Clone, PartialEq)]
struct LedFeedback {
    button_id: u8,
    r: u8,
    g: u8,
    b: u8,
}

#[derive(Debug, Clone, PartialEq)]
struct LightCommand {
    r: u8,
    g: u8,
    b: u8,
}

#[derive(Debug, Clone)]
enum SensorCommand {
    SendValue(u16),
    Shutdown,
}

#[derive(Debug, Deserialize)]
struct HubState {
    lights: Option<HashMap<String, LightState>>,
    buttons: Option<HashMap<String, serde_json::Value>>,
    sensors: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct LightState {
    r: u8,
    g: u8,
    b: u8,
}

async fn connect_and_handshake(device_type: u8, device_id: u8) -> Result<TcpStream, String> {
    let mut stream = TcpStream::connect((HOST, PORT))
        .await
        .map_err(|e| format!("Connection failed: {e}"))?;

    stream
        .write_all(&[HANDSHAKE_MAGIC, device_type, device_id])
        .await
        .map_err(|e| format!("Handshake send failed: {e}"))?;

    let mut ack = [0u8; 1];
    timeout(TIMEOUT, stream.read_exact(&mut ack))
        .await
        .map_err(|_| "Timeout waiting for handshake ACK".to_string())?
        .map_err(|e| format!("ACK read failed: {e}"))?;

    if ack[0] != HANDSHAKE_ACK {
        return Err(format!(
            "Bad ACK: expected 0x{:02X}, received 0x{:02X}",
            HANDSHAKE_ACK, ack[0]
        ));
    }

    Ok(stream)
}

async fn controller_task(
    stream: TcpStream,
    mut cmd_rx: mpsc::Receiver<ControllerCommand>,
    event_tx: mpsc::Sender<LedFeedback>,
) {
    let (mut reader, mut writer) = tokio::io::split(stream);

    // Spawn a reader subtask
    let event_tx_clone = event_tx.clone();
    let read_handle = tokio::spawn(async move {
        let mut buf = [0u8; 5]; // [TAG_LED_FEEDBACK, button_id, r, g, b]
        loop {
            match reader.read_exact(&mut buf).await {
                Ok(_) => {
                    if buf[0] == TAG_LED_FEEDBACK {
                        let fb = LedFeedback {
                            button_id: buf[1],
                            r: buf[2],
                            g: buf[3],
                            b: buf[4],
                        };
                        if event_tx_clone.send(fb).await.is_err() {
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Writer loop: send commands from orchestrator
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            ControllerCommand::ButtonPress(btn) => {
                if writer
                    .write_all(&[TAG_BUTTON_EVENT, btn, 0x01])
                    .await
                    .is_err()
                {
                    break;
                }
                let _ = writer.flush().await;
            }
            ControllerCommand::ButtonRelease(btn) => {
                if writer
                    .write_all(&[TAG_BUTTON_EVENT, btn, 0x00])
                    .await
                    .is_err()
                {
                    break;
                }
                let _ = writer.flush().await;
            }
            ControllerCommand::Shutdown => break,
        }
    }

    read_handle.abort();
}

async fn light_task(stream: TcpStream, event_tx: mpsc::Sender<LightCommand>) {
    let mut reader = tokio::io::BufReader::new(stream);
    let mut buf = [0u8; 4]; // [TAG_LIGHT_CMD, r, g, b]

    loop {
        match reader.read_exact(&mut buf).await {
            Ok(_) => {
                if buf[0] == TAG_LIGHT_CMD {
                    let cmd = LightCommand {
                        r: buf[1],
                        g: buf[2],
                        b: buf[3],
                    };
                    if event_tx.send(cmd).await.is_err() {
                        break;
                    }
                }
            }
            Err(_) => break,
        }
    }
}

async fn sensor_task(stream: TcpStream, mut cmd_rx: mpsc::Receiver<SensorCommand>) {
    let mut writer = tokio::io::BufWriter::new(stream);

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            SensorCommand::SendValue(val) => {
                let mut buf = [0u8; 3];
                buf[0] = TAG_SENSOR_VALUE;
                buf[1..3].copy_from_slice(&val.to_be_bytes());
                if writer.write_all(&buf).await.is_err() {
                    break;
                }
                if writer.flush().await.is_err() {
                    break;
                }
            }
            SensorCommand::Shutdown => break,
        }
    }
}

async fn query_state() -> Result<HubState, String> {
    let mut stream = connect_and_handshake(DEVICE_MONITOR, 0x00).await?;

    stream
        .write_all(&[TAG_STATE_QUERY])
        .await
        .map_err(|e| format!("Query send failed: {e}"))?;

    let mut len_buf = [0u8; 4];
    timeout(TIMEOUT, stream.read_exact(&mut len_buf))
        .await
        .map_err(|_| "Timeout waiting for state response".to_string())?
        .map_err(|e| format!("Response size read failed: {e}"))?;

    let json_len = u32::from_be_bytes(len_buf) as usize;
    if json_len > 1_000_000 {
        return Err(format!("Suspicious JSON size: {json_len} bytes"));
    }

    let mut json_buf = vec![0u8; json_len];
    timeout(TIMEOUT, stream.read_exact(&mut json_buf))
        .await
        .map_err(|_| "Timeout waiting for JSON body".to_string())?
        .map_err(|e| format!("JSON body read failed: {e}"))?;

    let json_str = String::from_utf8(json_buf).map_err(|e| format!("JSON not UTF-8: {e}"))?;

    let state: HubState = serde_json::from_str(&json_str)
        .map_err(|e| format!("Invalid JSON: {e}\nReceived: {json_str}"))?;

    Ok(state)
}

struct TestRunner {
    passed: usize,
    failed: usize,
    total: usize,
}

impl TestRunner {
    fn new() -> Self {
        Self {
            passed: 0,
            failed: 0,
            total: 8,
        }
    }

    fn pass(&mut self, phase: usize, name: &str) {
        self.passed += 1;
        println!("Phase {phase}: {name}");
    }

    fn fail(&mut self, phase: usize, name: &str, reason: &str) {
        self.failed += 1;
        println!("Phase {phase}: {name}");
        println!("| {reason}");
    }

    fn summary(&self) {
        println!();
        if self.failed == 0 {
            println!("{}/{} - All tests pass!", self.passed, self.total);
        } else {
            println!(
                "{}/{} - {} test(s) failed.",
                self.passed, self.total, self.failed
            );
        }
    }
}

fn drain<T>(rx: &mut mpsc::Receiver<T>) {
    while rx.try_recv().is_ok() {}
}

#[tokio::main]
async fn main() {
    println!("Device Hub Checker");
    println!("Connecting to {}:{}\n", HOST, PORT);

    let mut runner = TestRunner::new();

    // ---- INIT: connect / handshake

    let ctrl_stream = connect_and_handshake(DEVICE_CONTROLLER, 0x00).await;
    let light_stream = connect_and_handshake(DEVICE_LIGHT, 0x00).await;
    let sensor_stream = connect_and_handshake(DEVICE_SENSOR, 0x00).await;

    let (ctrl_stream, light_stream, sensor_stream) =
        match (ctrl_stream, light_stream, sensor_stream) {
            (Ok(c), Ok(l), Ok(s)) => {
                runner.pass(1, "Connection & Handshake (Controller, Light, Sensor)");
                (c, l, s)
            }
            (c, l, s) => {
                let mut reasons = Vec::new();
                if let Err(e) = &c {
                    reasons.push(format!("Controller: {e}"));
                }
                if let Err(e) = &l {
                    reasons.push(format!("Light: {e}"));
                }
                if let Err(e) = &s {
                    reasons.push(format!("Sensor: {e}"));
                }
                runner.fail(1, "Connection & Handshake", &reasons.join(" | "));
                println!("\nUnable to continue without connections. Stopping.");
                runner.summary();
                std::process::exit(1);
            }
        };

    // Spawn device tasks with channels
    let (ctrl_cmd_tx, ctrl_cmd_rx) = mpsc::channel::<ControllerCommand>(32);
    let (ctrl_event_tx, mut ctrl_event_rx) = mpsc::channel::<LedFeedback>(32);
    let (light_event_tx, mut light_event_rx) = mpsc::channel::<LightCommand>(32);
    let (sensor_cmd_tx, sensor_cmd_rx) = mpsc::channel::<SensorCommand>(32);

    tokio::spawn(controller_task(ctrl_stream, ctrl_cmd_rx, ctrl_event_tx));
    tokio::spawn(light_task(light_stream, light_event_tx));
    tokio::spawn(sensor_task(sensor_stream, sensor_cmd_rx));

    // Small delay to let everything settle
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ----- BTN 0 PRESS | Light receives RED

    ctrl_cmd_tx
        .send(ControllerCommand::ButtonPress(0))
        .await
        .ok();

    match timeout(TIMEOUT, light_event_rx.recv()).await {
        Ok(Some(cmd)) if cmd.r == 255 && cmd.g == 0 && cmd.b == 0 => {
            runner.pass(2, "Button 0 | Light receives RED (255,0,0)");
        }
        Ok(Some(cmd)) => {
            runner.fail(
                2,
                "Button 0 | Light receives RED",
                &format!(
                    "Wrong color: ({},{},{}) instead of (255,0,0)",
                    cmd.r, cmd.g, cmd.b
                ),
            );
        }
        Ok(None) => {
            runner.fail(
                2,
                "Button 0 | Light receives RED",
                "Channel closed (light disconnected?)",
            );
        }
        Err(_) => {
            runner.fail(
                2,
                "Button 0 | Light receives RED",
                "Timeout - no command received by Light in 2s",
            );
        }
    }

    // ---- LED Feedback - white for btn 0

    match timeout(TIMEOUT, ctrl_event_rx.recv()).await {
        Ok(Some(fb)) if fb.button_id == 0 && fb.r == 255 && fb.g == 255 && fb.b == 255 => {
            runner.pass(3, "LED Feedback - Controller receives white for button 0");
        }
        Ok(Some(fb)) => {
            runner.fail(
                3,
                "LED Feedback",
                &format!(
                    "Wrong feedback: button={}, color=({},{},{}) instead of button=0, (255,255,255)",
                         fb.button_id, fb.r, fb.g, fb.b
                ),
            );
        }
        Ok(None) => {
            runner.fail(3, "LED Feedback", "Channel closed");
        }
        Err(_) => {
            runner.fail(
                3,
                "LED Feedback",
                "Timeout - no LED feedback received in 2s",
            );
        }
    }

    // ---- BTN 1 - Light green

    ctrl_cmd_tx
        .send(ControllerCommand::ButtonPress(1))
        .await
        .ok();

    // expect both a light command AND an LED feedback. (collect both so the led doesn't pollute phase 5)
    let mut got_light = false;
    let mut phase4_pass = false;
    let mut phase4_reason = String::new();

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let mut events_collected = 0;

    while events_collected < 2 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        tokio::select! {
            evt = light_event_rx.recv(), if !got_light => {
                match evt {
                    Some(cmd) => {
                        got_light = true;
                        events_collected += 1;
                        if cmd.r == 0 && cmd.g == 255 && cmd.b == 0 {
                            phase4_pass = true;
                        } else {
                            phase4_reason = format!(
                                "Wrong color: ({},{},{}) instead of (0,255,0)",
                                                    cmd.r, cmd.g, cmd.b
                            );
                        }
                    }
                    None => break,
                }
            }
            _led = ctrl_event_rx.recv() => {
                events_collected += 1;
            }
            _ = tokio::time::sleep(remaining) => break,
        }
    }

    if phase4_pass {
        runner.pass(4, "Button 1 | Light receives GREEN (0,255,0)");
    } else if !got_light {
        runner.fail(
            4,
            "Button 1 | Light receives GREEN",
            "Timeout - no Light command received in 2s",
        );
    } else {
        runner.fail(4, "Button 1 | Light receives GREEN", &phase4_reason);
    }

    // ---- BTN 0 RELEASE

    // Drain any leftover events
    drain(&mut ctrl_event_rx);

    ctrl_cmd_tx
        .send(ControllerCommand::ButtonRelease(0))
        .await
        .ok();

    match timeout(TIMEOUT, ctrl_event_rx.recv()).await {
        Ok(Some(fb)) if fb.button_id == 0 && fb.r == 0 && fb.g == 0 && fb.b == 0 => {
            runner.pass(5, "Button 0 released | LED off (0,0,0)");
        }
        Ok(Some(fb)) => {
            runner.fail(
                5,
                "Button 0 released | LED off",
                &format!(
                    "Wrong feedback: button={}, color=({},{},{}) instead of button=0, (0,0,0)",
                    fb.button_id, fb.r, fb.g, fb.b
                ),
            );
        }
        Ok(None) => {
            runner.fail(5, "Button 0 released | LED off", "Channel closed");
        }
        Err(_) => {
            runner.fail(
                5,
                "Button 0 released | LED off",
                "Timeout - no feedback received in 2s",
            );
        }
    }

    // ---- SENSOR -> 150

    match sensor_cmd_tx.send(SensorCommand::SendValue(150)).await {
        Ok(_) => {
            // Give the hub a moment to process and store the value
            tokio::time::sleep(Duration::from_millis(100)).await;
            runner.pass(6, "Sensor sends value 150");
        }
        Err(_) => {
            runner.fail(
                6,
                "Sensor sends value 150",
                "Channel closed (sensor disconnected?)",
            );
        }
    }

    // ---- MONITOR STATE QUERY

    drain(&mut light_event_rx);
    drain(&mut ctrl_event_rx);

    match query_state().await {
        Ok(state) => {
            let mut errors = Vec::new();

            // Check light 0 = GREEN (last press was button 1 in phase 4)
            if let Some(lights) = &state.lights {
                if let Some(light0) = lights.get("0") {
                    if light0.r != 0 || light0.g != 255 || light0.b != 0 {
                        errors.push(format!(
                            "Light 0: ({},{},{}) instead of (0,255,0)",
                            light0.r, light0.g, light0.b
                        ));
                    }
                } else {
                    errors.push("Light 0 missing from state".to_string());
                }
            } else {
                errors.push("Field 'lights' missing".to_string());
            }

            // Check buttons: button 0 should be released, button 1 should be pressed
            if let Some(buttons) = &state.buttons {
                // Button 0 = released
                if let Some(val) = buttons.get("0") {
                    let s = val.as_str().unwrap_or("");
                    if s != "released" {
                        errors.push(format!("Button 0: '{s}' instead of 'released'"));
                    }
                }
                // Button 1 = pressed
                if let Some(val) = buttons.get("1") {
                    let s = val.as_str().unwrap_or("");
                    if s != "pressed" {
                        errors.push(format!("Button 1: '{s}' instead of 'pressed'"));
                    }
                }
            } else {
                errors.push("Field 'buttons' missing".to_string());
            }

            // Check sensor 0 = 150
            if let Some(sensors) = &state.sensors {
                if let Some(val) = sensors.get("0") {
                    let num = val.as_u64().or_else(|| val.as_f64().map(|f| f as u64));
                    if num != Some(150) {
                        errors.push(format!("Sensor 0: {val} instead of 150"));
                    }
                } else {
                    errors.push("Sensor 0 missing from state".to_string());
                }
            } else {
                errors.push("Field 'sensors' missing".to_string());
            }

            if errors.is_empty() {
                runner.pass(7, "State Query - Consistent JSON");
            } else {
                runner.fail(7, "State Query", &errors.join("; "));
            }
        }
        Err(e) => {
            runner.fail(7, "State Query", &e);
        }
    }

    // ---- RAPID SEQUENCE

    // Drain channels
    drain(&mut light_event_rx);
    drain(&mut ctrl_event_rx);

    // Send 3 button presses rapidly
    for btn in 0u8..=2 {
        ctrl_cmd_tx
            .send(ControllerCommand::ButtonPress(btn))
            .await
            .ok();
        // Tiny gap to preserve ordering on the wire
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let expected_colors: [(u8, u8, u8); 3] = [
        (255, 0, 0), // button 0 RED
        (0, 255, 0), // button 1 GREEN
        (0, 0, 255), // button 2 BLUE
    ];

    let mut phase8_pass = true;
    let mut phase8_reason = String::new();

    // Collect 3 light commands (large timeout)
    let deadline = tokio::time::Instant::now() + TIMEOUT + TIMEOUT;
    let mut received_colors: Vec<(u8, u8, u8)> = Vec::new();

    while received_colors.len() < 3 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        tokio::select! {
            evt = light_event_rx.recv() => {
                match evt {
                    Some(cmd) => received_colors.push((cmd.r, cmd.g, cmd.b)),
                    None => break,
                }
            }
            // drain LED feedbacks so they don't clog the controller channel
            _led = ctrl_event_rx.recv() => {}
            _ = tokio::time::sleep(remaining) => break,
        }
    }

    if received_colors.len() < 3 {
        phase8_pass = false;
        phase8_reason = format!(
            "Only {}/3 Light commands received (timeout)",
            received_colors.len()
        );
    } else {
        for (i, (got, expected)) in received_colors
            .iter()
            .zip(expected_colors.iter())
            .enumerate()
        {
            if got != expected {
                phase8_pass = false;
                phase8_reason = format!(
                    "Command {}: ({},{},{}) instead of ({},{},{})",
                    i, got.0, got.1, got.2, expected.0, expected.1, expected.2
                );
                break;
            }
        }
    }

    if phase8_pass {
        runner.pass(8, "Rapid sequence - 3 Light commands in order");
    } else {
        runner.fail(8, "Rapid sequence", &phase8_reason);
    }

    // Cleanup
    ctrl_cmd_tx.send(ControllerCommand::Shutdown).await.ok();
    sensor_cmd_tx.send(SensorCommand::Shutdown).await.ok();

    runner.summary();

    if runner.failed > 0 {
        std::process::exit(1);
    }
}
