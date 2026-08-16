use super::protocol::Color;
use serde::Serialize;
use serde::Serializer;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// Main Actor that contain the hub global state
/// Receive messages via `mpsc` channel & respond via `oneshot` channel
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
