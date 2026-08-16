use serde::Serialize;

pub const HANDSHAKE_MAGIC: u8 = 0xA0;
pub const HANDSHAKE_ACK: u8 = 0xAC;

pub const TAG_BUTTON_EVENT: u8 = 0x01;
pub const TAG_LED_FEEDBACK: u8 = 0x02;
pub const TAG_LIGHT_CMD: u8 = 0x03;
pub const TAG_SENSOR_VALUE: u8 = 0x04;
pub const TAG_STATE_QUERY: u8 = 0x10;

/// Devices type (identified upon the handshake)
#[derive(Debug)]
pub enum DeviceType {
    Controller, // 0x01
    Light,      // 0x02
    Sensor,     // 0x03
    Monitor,    // 0xFF
}

impl DeviceType {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Controller),
            0x02 => Some(Self::Light),
            0x03 => Some(Self::Sensor),
            0xFF => Some(Self::Monitor),
            _ => None,
        }
    }
}

/// Button actions
pub enum ButtonAction {
    Pressed,  // 0x01
    Released, // 0x00
}

impl ButtonAction {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Pressed),
            0x00 => Some(Self::Released),
            _ => None,
        }
    }
}

/// light colors RGB .
#[derive(Clone, Copy, Default, Serialize)]
pub struct Color {
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

impl Color {
    pub const OFF: Self = Self { r: 0, g: 0, b: 0 };
    pub const WHITE: Self = Self {
        r: 255,
        g: 255,
        b: 255,
    };
    pub const RED: Self = Self { r: 255, g: 0, b: 0 };
    pub const GREEN: Self = Self { r: 0, g: 255, b: 0 };
    pub const BLUE: Self = Self { r: 0, g: 0, b: 255 };
}
