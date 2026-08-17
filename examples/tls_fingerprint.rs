//! Reports the TLS fingerprint this deployment's HTTP client actually presents.
//!
//! There is no way to reason about a JA4 from the source: it is a property of
//! the TLS library's ClientHello, and the only honest way to know it is to send
//! one and read it back. Point this and a real browser at the same endpoint and
//! compare — measuring both with one instrument is the whole discipline.
//!
//! For reference, measured against `tls.browserleaks.com` on 2026-08-17:
//!   real Chrome (Playwright)  t13d1516h2_8daaf6152771_d8a2da3f94cd
//!   curl_cffi "chrome142"     t13d1516h2_8daaf6152771_d8a2da3f94cd
//!
//! Run: `cargo run --example tls_fingerprint -- [proxy-url]`

use mjai_management::majsoul::rpc::requests::{browser_headers, persona, user_agent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let proxy = std::env::args().nth(1);
    let machine = persona("someone@example.com");

    let mut builder = reqwest::Client::builder()
        .user_agent(user_agent(machine.chrome))
        .default_headers(browser_headers(machine.chrome));
    if let Some(proxy) = proxy.as_deref() {
        builder = builder.proxy(reqwest::Proxy::all(proxy)?);
        println!("(through {proxy})");
    }
    let http = builder.build()?;

    let body: serde_json::Value = http
        .get("https://tls.browserleaks.com/json")
        .send()
        .await?
        .json()
        .await?;

    for key in ["ja3_hash", "ja4", "akamai_hash", "user_agent"] {
        println!(
            "{key:12} {}",
            body.get(key).and_then(|v| v.as_str()).unwrap_or("-")
        );
    }
    Ok(())
}
