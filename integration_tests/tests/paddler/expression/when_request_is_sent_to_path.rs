use anyhow::Result;
use cucumber::gherkin::Step;
use cucumber::when;
use reqwest::Method;

use crate::paddler_world::PaddlerWorld;

#[when(expr = "request {string} is sent to {string} with:")]
pub async fn when_request_is_sent_to_path(
    world: &mut PaddlerWorld,
    step: &Step,
    name: String,
    path: String,
) -> Result<()> {
    let client = reqwest::Client::new();
    let mut request_builder = client.get(format!("http://127.0.0.1:8096{path}"));
    request_builder = request_builder.header("X-Request-Name", name.clone());

    if let Some(table) = step.table.as_ref() {
        for row in table.rows.iter() {
            let key = row[0].trim();
            let value = row[1].trim();

            match key.to_lowercase().as_str() {
                "method" => {
                    let method = value.to_uppercase();
                    request_builder = match method.as_str() {
                        "GET" => client.get(format!("http://127.0.0.1:8096{path}")),
                        "POST" => client.post(format!("http://127.0.0.1:8096{path}")),
                        "PUT" => client.put(format!("http://127.0.0.1:8096{path}")),
                        "DELETE" => client.delete(format!("http://127.0.0.1:8096{path}")),
                        "PATCH" => client.patch(format!("http://127.0.0.1:8096{path}")),
                        _ => client.request(method.parse::<Method>()?, format!("http://127.0.0.1:8096{path}")),
                    };
                    request_builder = request_builder.header("X-Request-Name", name.clone());
                }
                "header" => {
                    let parts: Vec<&str> = value.splitn(2, ':').collect();
                    if parts.len() == 2 {
                        let header_name = parts[0].trim();
                        let header_value = parts[1].trim();
                        request_builder = request_builder.header(header_name, header_value);
                    }
                }
                "body" => {
                    request_builder = request_builder.body(value.to_string());
                }
                _ => {}
            }
        }
    }

    let response = request_builder.send().await?;

    world.responses.insert(name, response);

    Ok(())
}
