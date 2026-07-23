//! Version-sensitive Mahjong Soul protocol implementation.
//!
//! The built-in adapter is derived from majsoul2mjai at commit
//! `da98580990279003f0bf0d636d0d6b8fae19a8cd`. See NOTICE.majsoul2mjai.

#![allow(clippy::all)]

pub mod auth;
pub mod convert;
pub mod events;
pub mod gateway;
pub mod modes;
pub mod proto;
pub mod rpc;
pub mod status;
pub mod tiles;

pub const BROWSER_USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36";
