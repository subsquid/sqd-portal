use sqd_contract_client::PeerId;

#[derive(serde::Serialize)]
pub(crate) struct PortalConfigApiResponse {
    pub peer_id: PeerId,
}

#[derive(serde::Serialize)]
pub(crate) struct AvailableDatasetApiResponse {
    pub slug: String,
    pub aliases: Vec<String>,
    pub real_time: bool,
}
