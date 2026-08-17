//! Version-sensitive Mahjong Soul protocol implementation.
//!
//! The built-in adapter is derived from majsoul2mjai at commit
//! `da98580990279003f0bf0d636d0d6b8fae19a8cd`. See NOTICE.majsoul2mjai.

pub mod auth;
pub mod convert;
pub mod events;
pub mod gateway;
pub mod modes;
pub mod proto;
pub mod rpc;
pub mod status;
pub mod telemetry;
pub mod tiles;

/// An account's device id: the `random_key` its login carries, and the
/// `device_id` its telemetry reports. One value, derived from the account name.
///
/// Formatted as a uuid because that is what the client sends, but it is not
/// random — a fresh uuid per login, which is what this used to do, describes an
/// account that gets a new machine every time it reconnects. A pool account
/// reconnecting every few minutes would have announced hundreds of them a day.
///
/// Derived rather than stored so there is no file to migrate and no way for the
/// two places that report it to disagree.
pub fn device_id(account: &str) -> String {
    // Sixteen bytes of SHA-256 over the account name, laid out as a v4 uuid:
    // version nibble 4, variant bits 10. Anything that parses these will see a
    // perfectly ordinary random uuid.
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(account.to_lowercase().as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).to_string()
}

// There is deliberately no fleet-wide user agent constant here any more.
//
// Every HTTP request this makes is on behalf of one account, moments before or
// after that account's login — and the login frame carries its own user agent in
// `device.f12`. One constant meant every session announced Chrome/150 over HTTP
// and whatever its persona said over the socket: two versions of one browser
// within a second, which no real client can produce. Both now come from
// [`rpc::requests::persona`] and [`rpc::requests::user_agent`], keyed by the
// account, so there is one answer per machine and no way to set only half of it.
