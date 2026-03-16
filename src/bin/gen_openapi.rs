use utoipa::OpenApi;

fn main() {
    print!("{}", sqd_portal::openapi::ApiDoc::openapi().to_pretty_json().unwrap());
}
