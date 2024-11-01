#[derive(serde::Serialize)]
pub(crate) struct AvailableDatasetApiResponse {
    pub slug: String,
    pub aliases: Vec<String>,
    pub real_time: bool,
}
