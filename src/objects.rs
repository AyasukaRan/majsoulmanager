use std::{fmt::Write as _, time::Duration};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::{Method, StatusCode};
use sha2::{Digest, Sha256};
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

/// SHA-256 of the empty string, which is the payload hash every request without
/// a body has to sign. RustFS, MinIO and S3 all reject a request to the `s3`
/// service that omits `x-amz-content-sha256` entirely, so this is not optional.
const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// A page of `ListObjectsV2` is capped at 1000 keys by every implementation, so
/// asking for more only costs a longer canonical query string.
const LIST_PAGE_SIZE: &str = "1000";

/// Minimal S3 client covering exactly the five operations the pack pipeline
/// needs. An AWS SDK would drag in a tree the size of the rest of this binary
/// to sign four request shapes, and `reqwest`, `hmac`, `sha2` and `hex` are all
/// already here for the same reason `clickhouse.rs` hand-writes its client.
///
/// Addressing is path-style, because the endpoint is a bare `host:port` and
/// `bucket.host` would not resolve. Pointing this at real AWS would need
/// virtual-host addressing, which is a change to `path` and `host` only.
pub struct Objects {
    http: reqwest::Client,
    uploads: reqwest::Client,
    endpoint: String,
    /// The `Host` header `reqwest` will derive from the endpoint, port included
    /// when it is not the scheme default. It has to be signed byte-identically
    /// to what goes on the wire.
    host: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
}

/// One `<Contents>` entry from a bucket listing, which is everything the GC
/// needs to decide whether an object is an orphan old enough to remove.
#[derive(Clone, Debug)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum ObjectError {
    #[error("object storage is unreachable: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("object storage rejected the request with HTTP {status}: {body}")]
    Server { status: StatusCode, body: String },
    #[error("object storage returned an unusable response: {0}")]
    Malformed(String),
}

impl Objects {
    pub fn new(
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Self, ObjectError> {
        let endpoint = endpoint.trim_end_matches('/').to_owned();
        let parsed = reqwest::Url::parse(&endpoint)
            .map_err(|error| ObjectError::Malformed(format!("S3 endpoint {endpoint}: {error}")))?;
        let authority = parsed
            .host_str()
            .ok_or_else(|| ObjectError::Malformed(format!("S3 endpoint {endpoint} has no host")))?;
        // `Url::port` is `None` for the scheme's default port, which is exactly
        // when `reqwest` omits the port from the `Host` header it sends.
        let host = match parsed.port() {
            Some(port) => format!("{authority}:{port}"),
            None => authority.to_owned(),
        };
        Ok(Self {
            // reqwest still has no default timeout, and the ClickHouse client
            // next door already documents what that costs. A sealed pack is up
            // to 256MB, so an upload cannot share a deadline sized for reading
            // one Zstandard frame back out; two clients is cheaper than one
            // per-request override that is easy to forget.
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(30))
                .build()?,
            uploads: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(600))
                .build()?,
            endpoint,
            host,
            bucket: bucket.to_owned(),
            region: region.to_owned(),
            access_key: access_key.to_owned(),
            secret_key: secret_key.to_owned(),
        })
    }

    /// Uploads a sealed pack. The payload is hashed rather than sent as
    /// `UNSIGNED-PAYLOAD` so the server rejects a body that was truncated or
    /// corrupted in flight instead of storing it; one SHA-256 pass over 256MB
    /// is a fraction of the transfer it protects.
    pub async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), ObjectError> {
        let payload_hash = hex::encode(Sha256::digest(&bytes));
        let response = self
            .signed(
                &self.uploads,
                Method::PUT,
                &self.object_path(key),
                "",
                &[],
                &payload_hash,
            )
            .body(bytes)
            .send()
            .await?;
        Self::success(response).await.map(drop)
    }

    /// Reads one slice of an object. `length` of zero returns nothing rather
    /// than sending `bytes=n-(n-1)`, which S3 treats as malformed and answers
    /// with the whole object.
    pub async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, ObjectError> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let last = offset + length - 1;
        let response = self
            .signed(
                &self.http,
                Method::GET,
                &self.object_path(key),
                "",
                &[("range", format!("bytes={offset}-{last}"))],
                EMPTY_PAYLOAD_SHA256,
            )
            .send()
            .await?;
        let response = Self::success(response).await?;
        // S3 silently ignores a Range header it cannot parse and returns 200
        // with the entire object, so a caller that trusted the body would hand
        // a whole pack to a Zstandard decoder expecting one frame. Anything but
        // a 206 naming the range we asked for is a bug, not a slow path.
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(ObjectError::Malformed(format!(
                "range request for {key} answered with HTTP {} instead of 206",
                response.status()
            )));
        }
        let expected = format!("bytes {offset}-{last}/");
        let content_range = response
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        if !content_range.starts_with(&expected) {
            return Err(ObjectError::Malformed(format!(
                "range request for {key} wanted {expected}* but got {content_range:?}"
            )));
        }
        let body = response.bytes().await?;
        if body.len() as u64 != length {
            return Err(ObjectError::Malformed(format!(
                "range request for {key} wanted {length} bytes but got {}",
                body.len()
            )));
        }
        Ok(body)
    }

    /// The object's size, or `None` when it does not exist. This is how an
    /// upload is confirmed to have landed before its index rows are written.
    pub async fn head(&self, key: &str) -> Result<Option<u64>, ObjectError> {
        let response = self
            .signed(
                &self.http,
                Method::HEAD,
                &self.object_path(key),
                "",
                &[],
                EMPTY_PAYLOAD_SHA256,
            )
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = Self::success(response).await?;
        // Read out of the header rather than through `Response::content_length`,
        // which reports the length of the decoded body and not the header:
        // hyper hard-codes a decoded length of zero for the response to any
        // HEAD, since a HEAD carries no body by definition. Asking reqwest for
        // it therefore answers `Some(0)` for every object that exists, which
        // makes each post-upload size check fail against a pack that landed
        // perfectly — the worker then discards, retries, and never indexes a
        // single record while leaving one orphan object per attempt.
        response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| {
                ObjectError::Malformed(format!("HEAD {key} answered without a Content-Length"))
            })
            .map(Some)
    }

    /// Every object under `prefix`, following continuation tokens to
    /// exhaustion. There is deliberately no way to ask for a single page: the
    /// only caller is the GC, and a listing that stopped early would make every
    /// unlisted object look like it had already been deleted.
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectEntry>, ObjectError> {
        let mut entries = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut params = vec![
                ("list-type", "2"),
                ("max-keys", LIST_PAGE_SIZE),
                ("prefix", prefix),
            ];
            if let Some(token) = &token {
                params.push(("continuation-token", token));
            }
            let response = self
                .signed(
                    &self.http,
                    Method::GET,
                    &format!("/{}", self.bucket),
                    &canonical_query(&params),
                    &[],
                    EMPTY_PAYLOAD_SHA256,
                )
                .send()
                .await?;
            let body = Self::success(response).await?.text().await?;
            let (page, next) = parse_list_page(&body)?;
            entries.extend(page);
            match next {
                // A server that keeps handing back the token it was given would
                // otherwise spin here forever, accumulating duplicates.
                Some(next) if Some(&next) == token.as_ref() => {
                    return Err(ObjectError::Malformed(
                        "listing repeated its continuation token without advancing".to_owned(),
                    ));
                }
                Some(next) => token = Some(next),
                None => return Ok(entries),
            }
        }
    }

    /// Deleting a key that is already gone succeeds, which is what makes a
    /// re-run of the GC after a partial failure harmless.
    pub async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        let response = self
            .signed(
                &self.http,
                Method::DELETE,
                &self.object_path(key),
                "",
                &[],
                EMPTY_PAYLOAD_SHA256,
            )
            .send()
            .await?;
        Self::success(response).await.map(drop)
    }

    fn object_path(&self, key: &str) -> String {
        format!("/{}/{}", self.bucket, key.trim_start_matches('/'))
    }

    /// Builds a request whose URL, headers and Authorization all agree with the
    /// canonical request that was signed. `path` is unencoded here and encoded
    /// once, by the same function the signer uses, because a request line that
    /// percent-encodes even one byte differently from what was signed is a
    /// `SignatureDoesNotMatch` with no other symptom.
    fn signed(
        &self,
        http: &reqwest::Client,
        method: Method,
        path: &str,
        query: &str,
        extra_headers: &[(&'static str, String)],
        payload_hash: &str,
    ) -> reqwest::RequestBuilder {
        // Only UTC: servers reject a signature whose timestamp is more than 15
        // minutes off their own clock, and the error looks nothing like one.
        let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let mut headers = vec![
            ("host", self.host.clone()),
            ("x-amz-content-sha256", payload_hash.to_owned()),
            ("x-amz-date", timestamp.clone()),
        ];
        headers.extend(extra_headers.iter().cloned());
        let (canonical, signed_headers) =
            canonical_request(method.as_str(), path, query, &headers, payload_hash);
        let signature = signature(&self.secret_key, &self.region, &timestamp, &canonical);
        let encoded_path = uri_encode(path, false);
        let url = if query.is_empty() {
            format!("{}{encoded_path}", self.endpoint)
        } else {
            format!("{}{encoded_path}?{query}", self.endpoint)
        };
        let mut request = http.request(method, url);
        for (name, value) in &headers {
            // reqwest derives Host from the URL; setting it again would send it
            // twice and break the signature we just computed over one copy.
            if *name != "host" {
                request = request.header(*name, value);
            }
        }
        request.header(
            reqwest::header::AUTHORIZATION,
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{}/{}/s3/aws4_request, \
                 SignedHeaders={signed_headers}, Signature={signature}",
                self.access_key,
                &timestamp[..8],
                self.region,
            ),
        )
    }

    /// Splits a transport failure from a server refusal, because the GC must
    /// treat "I could not ask" and "the bucket says no" differently.
    async fn success(response: reqwest::Response) -> Result<reqwest::Response, ObjectError> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        // S3 errors carry an XML document naming the code; it is worth far more
        // in a log line than the status alone, and it is always small.
        let body = response.text().await.unwrap_or_default();
        Err(ObjectError::Server {
            status,
            body: body.trim().to_owned(),
        })
    }
}

/// AWS's `UriEncode`: unreserved characters survive, everything else becomes
/// uppercase percent escapes, and a space is never a `+`. `encode_slash` is
/// false for a path, where `/` is a separator, and true inside a query
/// parameter, where a `/` in a continuation token is data.
fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(byte));
            }
            b'/' if !encode_slash => out.push('/'),
            _ => write!(out, "%{byte:02X}").expect("writing to a String cannot fail"),
        }
    }
    out
}

/// Sorting happens *after* encoding, which is why `list-type` lands between
/// `continuation-token` and `max-keys`: a token full of `/` and `=` sorts
/// differently before and after escaping.
fn canonical_query(params: &[(&str, &str)]) -> String {
    let mut encoded: Vec<(String, String)> = params
        .iter()
        .map(|(name, value)| (uri_encode(name, true), uri_encode(value, true)))
        .collect();
    encoded.sort();
    encoded
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Returns the canonical request and its `SignedHeaders` list. Header names are
/// assumed to be lowercase already and to appear at most once, which holds for
/// every caller here; duplicates would need combining into one comma-separated
/// value first.
fn canonical_request(
    method: &str,
    path: &str,
    query: &str,
    headers: &[(&str, String)],
    payload_hash: &str,
) -> (String, String) {
    let mut headers: Vec<(&str, String)> = headers
        .iter()
        // Values are trimmed and their internal runs of whitespace collapsed to
        // one space, matching what the server does before it re-signs.
        .map(|(name, value)| {
            (
                *name,
                value.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .collect();
    headers.sort_by(|left, right| left.0.cmp(right.0));
    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    // The header block already ends in a newline and the field separator adds
    // another, which is the blank line before SignedHeaders. There is no
    // trailing newline after the payload hash.
    let canonical = format!(
        "{method}\n{}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        uri_encode(path, false)
    );
    (canonical, signed_headers)
}

fn signature(secret_key: &str, region: &str, timestamp: &str, canonical: &str) -> String {
    let date = &timestamp[..8];
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{date}/{region}/s3/aws4_request\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let key = hmac(format!("AWS4{secret_key}").as_bytes(), date);
    let key = hmac(&key, region);
    let key = hmac(&key, "s3");
    let key = hmac(&key, "aws4_request");
    hex::encode(hmac(&key, &string_to_sign))
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC-SHA256 takes a key of any length");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Scans one `ListObjectsV2` page for its `<Contents>` entries and the token
/// that continues it.
///
/// This is a deliberately narrow scanner rather than an XML parser, because the
/// document is flat, machine-generated and fixed by the S3 API. What it assumes:
/// `<Contents>` blocks do not nest, the three elements it reads appear once
/// inside each block with no attributes, and text content uses only the five
/// predefined XML entities. A response that violates any of those is reported as
/// `Malformed` rather than guessed at — an unknown entity in a key, a
/// `<Contents>` block missing an element, a truncated page with no token. That
/// matters more than tolerance here, because the one caller is a GC and a key it
/// silently failed to see is a key it will delete.
fn parse_list_page(xml: &str) -> Result<(Vec<ObjectEntry>, Option<String>), ObjectError> {
    let mut entries = Vec::new();
    for block in xml.split("<Contents>").skip(1) {
        let block = block.split("</Contents>").next().unwrap_or_default();
        let key =
            unescape(element(block, "Key").ok_or_else(|| {
                ObjectError::Malformed("listing entry without a <Key>".to_owned())
            })?)?;
        let size = element(block, "Size")
            .and_then(|size| size.trim().parse::<u64>().ok())
            .ok_or_else(|| {
                ObjectError::Malformed(format!("listing entry {key} without a <Size>"))
            })?;
        let last_modified = element(block, "LastModified")
            .and_then(|stamp| DateTime::parse_from_rfc3339(stamp.trim()).ok())
            .ok_or_else(|| {
                ObjectError::Malformed(format!("listing entry {key} without a <LastModified>"))
            })?
            .with_timezone(&Utc);
        entries.push(ObjectEntry {
            key,
            size,
            last_modified,
        });
    }
    let truncated =
        element(xml, "IsTruncated").is_some_and(|flag| flag.trim().eq_ignore_ascii_case("true"));
    if !truncated {
        return Ok((entries, None));
    }
    // Driven off IsTruncated rather than <KeyCount>, which not every
    // S3-compatible server sends. A truncated page without a token is
    // unfollowable, and returning what we have would be a short list.
    let next = element(xml, "NextContinuationToken").ok_or_else(|| {
        ObjectError::Malformed("truncated listing without a <NextContinuationToken>".to_owned())
    })?;
    Ok((entries, Some(unescape(next)?)))
}

/// The text of the first `<tag>...</tag>`. Self-closing `<tag/>` has no text and
/// yields `None`, which is correct for every element read here.
fn element<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let start = xml.find(&format!("<{tag}>"))? + tag.len() + 2;
    let rest = &xml[start..];
    Some(&rest[..rest.find(&format!("</{tag}>"))?])
}

fn unescape(text: &str) -> Result<String, ObjectError> {
    const ENTITIES: [(&str, &str); 5] = [
        ("&amp;", "&"),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&quot;", "\""),
        ("&apos;", "'"),
    ];
    if !text.contains('&') {
        return Ok(text.to_owned());
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(index) = rest.find('&') {
        out.push_str(&rest[..index]);
        let tail = &rest[index..];
        // Numeric character references are legal XML and would need decoding,
        // but no key this service writes contains one, and a wrong guess here
        // hands the GC a key that matches nothing in the index and therefore
        // looks like an orphan. Refusing the page is the safe failure.
        let (entity, replacement) = ENTITIES
            .iter()
            .find(|(entity, _)| tail.starts_with(entity))
            .ok_or_else(|| {
                ObjectError::Malformed(format!("unsupported XML entity in listing text {text:?}"))
            })?;
        out.push_str(replacement);
        rest = &tail[entity.len()..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// From the S3 SigV4 documentation's worked examples. Note this secret ends
    /// `DENG/bPx`, where the one in the aws-c-auth suite ends `DENG+bPx`;
    /// crossing them makes correct code look broken.
    const DOC_SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const DOC_HOST: &str = "examplebucket.s3.amazonaws.com";
    const DOC_TIMESTAMP: &str = "20130524T000000Z";

    #[test]
    fn signs_the_documented_ranged_get() {
        let headers = [
            ("host", DOC_HOST.to_owned()),
            ("range", "bytes=0-9".to_owned()),
            ("x-amz-content-sha256", EMPTY_PAYLOAD_SHA256.to_owned()),
            ("x-amz-date", DOC_TIMESTAMP.to_owned()),
        ];
        let (canonical, signed_headers) =
            canonical_request("GET", "/test.txt", "", &headers, EMPTY_PAYLOAD_SHA256);
        assert_eq!(
            canonical,
            concat!(
                "GET\n",
                "/test.txt\n",
                "\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "range:bytes=0-9\n",
                "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b78\
                 52b855\n",
                "x-amz-date:20130524T000000Z\n",
                "\n",
                "host;range;x-amz-content-sha256;x-amz-date\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
        );
        assert_eq!(signed_headers, "host;range;x-amz-content-sha256;x-amz-date");
        assert_eq!(
            signature(DOC_SECRET, "us-east-1", DOC_TIMESTAMP, &canonical),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// The same documentation's PUT example, whose key needs percent-encoding:
    /// `$` is not unreserved, so the canonical URI says `%24` while the header
    /// block and the signed payload hash cover a real body.
    #[test]
    fn signs_the_documented_put_with_an_escaped_key() {
        let body_hash = "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072";
        let headers = [
            ("date", "Fri, 24 May 2013 00:00:00 GMT".to_owned()),
            ("host", DOC_HOST.to_owned()),
            ("x-amz-content-sha256", body_hash.to_owned()),
            ("x-amz-date", DOC_TIMESTAMP.to_owned()),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY".to_owned()),
        ];
        let (canonical, signed_headers) =
            canonical_request("PUT", "/test$file.text", "", &headers, body_hash);
        assert!(canonical.starts_with("PUT\n/test%24file.text\n\n"));
        assert_eq!(
            signed_headers,
            "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class"
        );
        assert_eq!(
            signature(DOC_SECRET, "us-east-1", DOC_TIMESTAMP, &canonical),
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
    }

    /// Sorting after encoding is what puts `list-type` in the middle; sorting
    /// the raw names would place it after `max-keys` because `-` precedes `2`
    /// only once the token's `/` has become `%2F`.
    #[test]
    fn canonicalises_a_list_objects_v2_query() {
        let query = canonical_query(&[
            ("prefix", "packs/2026/"),
            ("list-type", "2"),
            (
                "continuation-token",
                "1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=",
            ),
            ("max-keys", "1000"),
        ]);
        assert_eq!(
            query,
            "continuation-token=1ueGcxLPRx1Tr%2FXYExHnhbYLgveDs2J%2Fwm36Hy4vbOwM%3D\
             &list-type=2&max-keys=1000&prefix=packs%2F2026%2F"
        );
        let headers = [
            ("host", "rustfs:9000".to_owned()),
            ("x-amz-content-sha256", EMPTY_PAYLOAD_SHA256.to_owned()),
            ("x-amz-date", DOC_TIMESTAMP.to_owned()),
        ];
        let (canonical, _) =
            canonical_request("GET", "/mjai-raw", &query, &headers, EMPTY_PAYLOAD_SHA256);
        assert_eq!(
            canonical.lines().take(3).collect::<Vec<_>>(),
            vec!["GET", "/mjai-raw", &query]
        );
    }

    fn page(contents: &str, truncated: &str) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>mjai-raw</Name><Prefix>packs/</Prefix><KeyCount>1</KeyCount>\
             <MaxKeys>1000</MaxKeys>{truncated}{contents}</ListBucketResult>"
        )
    }

    #[test]
    fn reads_a_listing_page_and_its_continuation_token() {
        let contents = "<Contents><Key>packs/2026/07/27/0-a&amp;b.mjpack</Key>\
                        <LastModified>2026-07-27T10:11:12.000Z</LastModified>\
                        <ETag>\"70ee1738\"</ETag><Size>268435456</Size>\
                        <StorageClass>STANDARD</StorageClass></Contents>";
        let body = page(
            contents,
            "<IsTruncated>true</IsTruncated><NextContinuationToken>a/b=</NextContinuationToken>",
        );
        let (entries, next) = parse_list_page(&body).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, "packs/2026/07/27/0-a&b.mjpack");
        assert_eq!(entries[0].size, 268_435_456);
        assert_eq!(
            entries[0].last_modified.to_rfc3339(),
            "2026-07-27T10:11:12+00:00"
        );
        assert_eq!(next.as_deref(), Some("a/b="));

        let (entries, next) =
            parse_list_page(&page(contents, "<IsTruncated>false</IsTruncated>")).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(next, None);
    }

    #[test]
    fn refuses_a_page_it_cannot_follow_or_decode() {
        let truncated = page("", "<IsTruncated>true</IsTruncated>");
        assert!(matches!(
            parse_list_page(&truncated),
            Err(ObjectError::Malformed(_))
        ));
        let numeric_entity = page(
            "<Contents><Key>packs/&#38;.mjpack</Key>\
             <LastModified>2026-07-27T10:11:12.000Z</LastModified><Size>1</Size></Contents>",
            "<IsTruncated>false</IsTruncated>",
        );
        assert!(matches!(
            parse_list_page(&numeric_entity),
            Err(ObjectError::Malformed(_))
        ));
    }

    /// The size of a HEAD's object cannot come from `Response::content_length`:
    /// a HEAD carries no body, so the decoded length reqwest reports is always
    /// zero and every object would confirm as empty. That reads as a truncated
    /// upload at both call sites, and the pack worker's response to a truncated
    /// upload is to discard and retry forever, so the whole pipeline indexes
    /// nothing while the bucket fills with orphans. Nothing short of a real
    /// response over a socket catches it — a hand-built `HeaderMap` would not,
    /// because the bug is in how the body length is derived, not in the header.
    #[tokio::test]
    async fn head_reads_the_size_from_the_header_a_bodyless_response_carries() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for response in [
                "HTTP/1.1 200 OK\r\nContent-Length: 268435456\r\n\r\n",
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 1024];
                // One read is enough to know the request arrived; a HEAD has no
                // body, so there is nothing after the headers to drain.
                let read = socket.read(&mut request).await.unwrap();
                assert!(read > 0, "the client opened a connection and sent nothing");
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.flush().await.unwrap();
            }
        });

        let objects = Objects::new(&endpoint, "mjai-raw", "us-east-1", "key", "secret").unwrap();
        assert_eq!(
            objects.head("packs/2026/07/27/0-a.mjpack").await.unwrap(),
            Some(268_435_456)
        );
        assert_eq!(objects.head("packs/missing.mjpack").await.unwrap(), None);
        server.await.unwrap();
    }
}
