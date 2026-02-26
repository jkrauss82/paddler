use anyhow::Result;
use anyhow::anyhow;
use cucumber::then;

use crate::paddler_world::PaddlerWorld;

#[then(expr = "response code is {int}")]
pub async fn then_response_code_is_simple(
    world: &mut PaddlerWorld,
    expected_code: u16,
) -> Result<()> {
    let responses: Vec<_> = world.responses.iter().collect();
    if responses.is_empty() {
        return Err(anyhow!("No response found"));
    }

    // Get the last response (most recently added)
    let last_response = responses.last().unwrap();
    let response = last_response.value();

    let status = response.status();
    if status.as_u16() != expected_code {
        return Err(anyhow!(
            "Expected status code {expected_code}, but got {status}"
        ));
    }

    Ok(())
}
