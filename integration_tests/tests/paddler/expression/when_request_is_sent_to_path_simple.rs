use anyhow::Result;
use cucumber::when;

use crate::paddler_world::PaddlerWorld;

#[when(expr = "request {string} is sent to {string}")]
pub async fn when_request_is_sent_to_path_simple(
    world: &mut PaddlerWorld,
    name: String,
    path: String,
) -> Result<()> {
    let client = reqwest::Client::new();
    let request_builder = client.get(format!("http://127.0.0.1:8096{path}"));
    let response = request_builder
        .header("X-Request-Name", name.clone())
        .send()
        .await?;

    world.responses.insert(name, response);

    Ok(())
}