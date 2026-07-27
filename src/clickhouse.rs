use std::time::Duration;

use serde::de::DeserializeOwned;
use thiserror::Error;

/// Minimal client for the ClickHouse HTTP interface. The native protocol would
/// buy pipelining and a smaller wire format, but `reqwest` is already a
/// dependency and every statement this crate issues is either a DDL, a keyset
/// page or one bulk `INSERT`, so the extra crate would earn nothing.
pub struct ClickHouse {
    http: reqwest::Client,
    url: String,
    user: String,
    password: String,
}

#[derive(Debug, Error)]
pub enum ClickHouseError {
    #[error("ClickHouse is unreachable: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("ClickHouse rejected the statement: {0}")]
    Server(String),
    #[error("ClickHouse returned an unreadable row: {0}")]
    Decode(#[from] serde_json::Error),
}

impl ClickHouse {
    pub fn new(url: &str, user: &str, password: &str) -> Result<Self, ClickHouseError> {
        Ok(Self {
            // reqwest has no default timeout, so a ClickHouse that accepts the
            // connection and never answers would hang the startup wait past its
            // own deadline and stall every request behind it. The request
            // timeout has to clear a full insert batch, not just a point read.
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(30))
                .build()?,
            url: url.trim_end_matches('/').to_owned(),
            user: user.to_owned(),
            password: password.to_owned(),
        })
    }

    /// Statements travel in the `query` URL parameter and user input travels in
    /// `param_*` bindings referenced as `{name:Type}`, so a player name
    /// containing a quote cannot change the statement.
    pub async fn execute(
        &self,
        sql: &str,
        params: &[(&str, String)],
        body: String,
    ) -> Result<String, ClickHouseError> {
        let mut request = self
            .http
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.password))
            .query(&[("query", sql)]);
        for (name, value) in params {
            request = request.query(&[(format!("param_{name}"), value)]);
        }
        // ClickHouse answers HTTP 381 to a POST that carries neither a
        // Content-Length nor chunked encoding, and reqwest sends no length for
        // an empty body, which is every DDL and every SELECT here.
        let response = request
            .header(reqwest::header::CONTENT_LENGTH, body.len())
            .body(body)
            .send()
            .await?;
        let status = response.status();
        let payload = response.text().await?;
        if status.is_success() {
            Ok(payload)
        } else {
            Err(ClickHouseError::Server(payload.trim().to_owned()))
        }
    }

    pub async fn query<T: DeserializeOwned>(
        &self,
        sql: &str,
        params: &[(&str, String)],
    ) -> Result<Vec<T>, ClickHouseError> {
        // JSONEachRow quotes 64-bit integers by default, which would make
        // `pack_offset` and `uniqExact` arrive as strings.
        let payload = self
            .execute(
                &format!(
                    "{sql} SETTINGS output_format_json_quote_64bit_integers = 0 \
                     FORMAT JSONEachRow"
                ),
                params,
                String::new(),
            )
            .await?;
        payload
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).map_err(ClickHouseError::from))
            .collect()
    }

    /// Whether a skip index is already declared on a table. `ADD INDEX` is
    /// idempotent but silent, so this is the only way to tell an installation
    /// that needs its existing parts materialised from one that does not.
    pub async fn has_skip_index(
        &self,
        database: &str,
        table: &str,
        name: &str,
    ) -> Result<bool, ClickHouseError> {
        #[derive(serde::Deserialize)]
        struct Declared {
            declared: u8,
        }
        let rows: Vec<Declared> = self
            .query(
                "SELECT count() > 0 AS declared FROM system.data_skipping_indices \
                 WHERE database = {database:String} AND table = {table:String} \
                 AND name = {name:String}",
                &[
                    ("database", database.to_owned()),
                    ("table", table.to_owned()),
                    ("name", name.to_owned()),
                ],
            )
            .await?;
        Ok(rows.first().is_some_and(|row| row.declared == 1))
    }

    /// `rows` is newline-delimited JSONEachRow. One request per batch: every
    /// `INSERT` becomes a MergeTree part, and a part per row would trip
    /// `too many parts` long before the 641k historical records are through.
    pub async fn insert(&self, table: &str, rows: String) -> Result<(), ClickHouseError> {
        self.execute(
            &format!("INSERT INTO {table} FORMAT JSONEachRow"),
            &[],
            rows,
        )
        .await
        .map(drop)
    }
}
