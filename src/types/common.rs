pub type ResponseChunk = Vec<u8>;

pub type QueryId = String;

pub fn generate_query_id() -> QueryId {
    uuid::Uuid::new_v4().to_string()
}
