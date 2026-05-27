use sqd_portal_client::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://localhost:8000".to_string());

    let client = Client::new(&url);
    let status = client.get_status().await?;

    println!("{}", serde_json::to_string_pretty(status.as_ref())?);

    Ok(())
}
