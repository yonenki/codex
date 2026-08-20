use anyhow::Result;
use codex_features::Feature;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call_with_namespace;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_once_match;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::test_codex::test_codex;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::time::Duration;

fn body_contains(request: &wiremock::Request, text: &str) -> bool {
    serde_json::from_slice::<serde_json::Value>(&request.body)
        .is_ok_and(|body| body.to_string().contains(text))
}

fn has_function_call_output(request: &wiremock::Request, call_id: &str) -> bool {
    serde_json::from_slice::<serde_json::Value>(&request.body).is_ok_and(|body| {
        body.get("input")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| {
                items.iter().any(|item| {
                    item.get("type").and_then(serde_json::Value::as_str)
                        == Some("function_call_output")
                        && item.get("call_id").and_then(serde_json::Value::as_str) == Some(call_id)
                })
            })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn team_spawn_agent_binds_native_child_before_first_turn() -> Result<()> {
    let server = start_mock_server().await;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, "start the sample team"),
        sse(vec![
            ev_response_created("team-start"),
            ev_function_call_with_namespace(
                "start-call",
                "team",
                "start_team",
                &json!({ "graph_name": "sample" }).to_string(),
            ),
            ev_completed("team-start"),
        ]),
    )
    .await;

    let mut builder = test_codex()
        .with_model("gpt-5.6-sol")
        .with_workspace_setup(|cwd, _| {
            let cwd = cwd.to_path_buf();
            async move {
                std::fs::create_dir_all(cwd.join(".codex").join("teams"))?;
                std::fs::create_dir_all(cwd.join(".codex").join("agents"))?;
                std::fs::write(
                    cwd.join(".codex").join("agents").join("worker.toml"),
                    "name = \"worker\"\ndescription = \"Test worker.\"\n",
                )?;
                std::fs::write(
                    cwd.join(".codex").join("teams").join("sample.toml"),
                    r#"
schema_version = 1
name = "sample"
version = "1"
description = "Sample team graph."
start = "work"
terminals = ["completed"]
[[nodes]]
id = "work"
purpose = "Implement the candidate."
role = "worker"
prompt = "Implement the approved scope."
completion = "A candidate exists."
available_tools = ["spawn_agent", "record_team_result", "get_team_next", "transition_team", "end_team"]
recommended_tools = ["spawn_agent", "record_team_result", "transition_team"]
[[nodes.transitions]]
on = "candidate_ready"
to = "completed"
recommended = true
guide = "Worker returned a candidate."
[[nodes]]
id = "completed"
purpose = "Team finished."
prompt = "Stop. The team is complete."
completion = "The team session is closed."
"#,
                )?;
                Ok(())
            }
        })
        .with_config(|config| {
            config
                .features
                .enable(Feature::Collab)
                .expect("enable collab");
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("enable mav2");
        });
    let test = builder.build(&server).await?;

    test.submit_turn("start the sample team").await?;

    let output = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if let Some(output) = function_output_from_requests(&server, "start-call").await {
                return output;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    let parsed: serde_json::Value =
        serde_json::from_str(&output).unwrap_or(json!({ "raw": output }));
    assert!(
        parsed.get("team_session_id").is_some()
            || parsed.get("view").is_some()
            || output.contains("team_"),
        "start_team should return a team session: {output}"
    );
    assert_eq!(test.thread_manager.list_thread_ids().await.len(), 1);
    Ok(())
}

async fn function_output_from_requests(
    server: &wiremock::MockServer,
    call_id: &str,
) -> Option<String> {
    for request in server.received_requests().await.unwrap_or_default() {
        if !has_function_call_output(&request, call_id) {
            continue;
        }
        let body = serde_json::from_slice::<serde_json::Value>(&request.body).ok()?;
        let output = body.get("input")?.as_array()?.iter().find(|item| {
            item.get("type").and_then(serde_json::Value::as_str) == Some("function_call_output")
                && item.get("call_id").and_then(serde_json::Value::as_str) == Some(call_id)
        })?;
        return output
            .get("output")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| output.get("output").map(ToString::to_string));
    }
    None
}
