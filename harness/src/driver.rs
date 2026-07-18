//! Client driver: issues requests per IB-2/IB-3, captures raw status, headers,
//! and bytes, and decodes bodies per their declared encoding (validator 1).

use std::collections::HashMap;
use std::io::Read;

use anyhow::Context;
use serde_json::Value;

#[derive(Debug)]
pub struct Decoded {
    pub status: u16,
    /// Lowercased header names → first value.
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    /// Parsed JSONL lines when the body is line-delimited JSON.
    pub lines: Vec<Value>,
    /// Decode errors (bad encoding / torn lines) — validator 1 failures.
    pub decode_errors: Vec<String>,
}

impl Decoded {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_ascii_lowercase()).map(|s| s.as_str())
    }

    pub fn block_numbers(&self) -> Vec<u64> {
        self.lines
            .iter()
            .filter_map(|l| l["header"]["number"].as_u64())
            .collect()
    }
}

pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .unwrap()
}

async fn decode(resp: reqwest::Response) -> anyhow::Result<Decoded> {
    let status = resp.status().as_u16();
    let mut headers = HashMap::new();
    for (k, v) in resp.headers() {
        headers
            .entry(k.as_str().to_ascii_lowercase())
            .or_insert_with(|| v.to_str().unwrap_or("<binary>").to_string());
    }
    let raw = resp.bytes().await.context("reading body")?.to_vec();

    let mut decode_errors = Vec::new();
    let body = match headers.get("content-encoding").map(|s| s.as_str()) {
        Some("gzip") => {
            let mut out = Vec::new();
            let mut dec = flate2::read::MultiGzDecoder::new(raw.as_slice());
            if let Err(e) = dec.read_to_end(&mut out) {
                decode_errors.push(format!("gzip decode failed: {e}"));
                Vec::new()
            } else {
                out
            }
        }
        Some(other) if other != "identity" => {
            decode_errors.push(format!("unsupported content-encoding: {other}"));
            Vec::new()
        }
        _ => raw.clone(),
    };

    let mut lines = Vec::new();
    let is_jsonl = headers
        .get("content-type")
        .map(|c| c.contains("jsonl"))
        .unwrap_or(false);
    if is_jsonl && !body.is_empty() {
        for (i, line) in body.split(|b| *b == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<Value>(line) {
                Ok(v) => lines.push(v),
                Err(e) => decode_errors.push(format!("line {i} does not parse: {e}")),
            }
        }
    }

    Ok(Decoded { status, headers, body, lines, decode_errors })
}

pub async fn stream(
    client: &reqwest::Client,
    base: &str,
    alias: &str,
    endpoint: &str,
    query: &Value,
    request_id: &str,
) -> anyhow::Result<Decoded> {
    let resp = client
        .post(format!("{base}/datasets/{alias}/{endpoint}"))
        .header("accept-encoding", "gzip")
        .header("content-type", "application/json")
        .header("x-request-id", request_id)
        .body(query.to_string())
        .send()
        .await
        .context("stream request")?;
    decode(resp).await
}

pub async fn get(client: &reqwest::Client, url: &str) -> anyhow::Result<Decoded> {
    let resp = client
        .get(url)
        .header("accept-encoding", "gzip")
        .send()
        .await
        .context("get request")?;
    decode(resp).await
}
