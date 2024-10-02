use sqd_contract_client::PeerId;

#[derive(serde::Serialize)]
pub(crate) struct PortalConfigApiResponse {
    pub peer_id: PeerId,
}
