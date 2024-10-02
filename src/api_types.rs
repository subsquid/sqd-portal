use sqd_contract_client::PeerId;

#[derive(serde::Serialize)]
pub(crate) struct PortalConfigApiResponse {
    pub libp2p_key: PeerId,
}

#[derive(serde::Serialize)]
pub(crate) struct AvailableDatasetApiResponse {
    pub slug: String,
    pub aliases: Vec<String>,
    pub real_time: bool,
}
