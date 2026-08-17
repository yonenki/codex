// Aggregates all former standalone integration tests as modules.
use codex_apply_patch::CODEX_CORE_APPLY_PATCH_ARG1;
#[cfg(unix)]
use codex_exec_server::CODEX_ARG0_EXEC_HELPER_ARG1;
use codex_exec_server::CODEX_FS_HELPER_ARG1;
use codex_sandboxing::landlock::CODEX_LINUX_SANDBOX_ARG0;
use codex_test_binary_support::TestBinaryDispatchGuard;
use codex_test_binary_support::TestBinaryDispatchMode;
use codex_test_binary_support::configure_test_binary_dispatch;
use ctor::ctor;
use std::io::BufRead;
use std::io::Write;
use std::path::PathBuf;

const ACP_FIXTURE_EXECUTABLE: &str = "acp-harness-host";

fn install_acp_fixture(directory: &std::path::Path) -> std::io::Result<PathBuf> {
    let executable = directory.join(format!(
        "{ACP_FIXTURE_EXECUTABLE}{}",
        std::env::consts::EXE_SUFFIX
    ));
    std::fs::copy(std::env::current_exe()?, &executable)?;
    Ok(executable)
}

// This code runs before any other tests are run.
// It allows the test binary to behave like codex and dispatch to apply_patch and codex-linux-sandbox
// based on the arg0.
// NOTE: this doesn't work on ARM
#[ctor]
pub static CODEX_ALIASES_TEMP_DIR: Option<TestBinaryDispatchGuard> = {
    let args = std::env::args_os().collect::<Vec<_>>();
    let is_acp_fixture = args
        .windows(2)
        .any(|args| args[0] == "--harness" && args[1] == "grok-build");
    if is_acp_fixture {
        let has_model = args.windows(2).any(|args| {
            args[0] == "--model" && (args[1] == "grok-test" || args[1] == "grok-fallback-test")
        });
        let has_effort = args
            .windows(2)
            .any(|args| args[0] == "--effort" && args[1] == "xhigh");
        if !has_model || !has_effort {
            std::process::exit(2);
        }
        run_acp_fixture();
    }
    configure_test_binary_dispatch("codex-core-tests", |exe_name, argv1| {
        if argv1 == Some(CODEX_CORE_APPLY_PATCH_ARG1) {
            return TestBinaryDispatchMode::DispatchArg0Only;
        }
        #[cfg(unix)]
        if argv1 == Some(CODEX_ARG0_EXEC_HELPER_ARG1) {
            return TestBinaryDispatchMode::DispatchArg0Only;
        }
        if argv1 == Some(CODEX_FS_HELPER_ARG1) {
            return TestBinaryDispatchMode::DispatchArg0Only;
        }
        if exe_name == CODEX_LINUX_SANDBOX_ARG0 {
            return TestBinaryDispatchMode::DispatchArg0Only;
        }
        TestBinaryDispatchMode::InstallAliases
    })
};

fn run_acp_fixture() -> ! {
    let selected_model = std::env::args_os()
        .collect::<Vec<_>>()
        .windows(2)
        .find_map(|args| (args[0] == "--model").then(|| args[1].to_string_lossy().into_owned()))
        .unwrap_or_else(|| "default".to_string());
    let stdin = std::io::stdin();
    let mut stdout = std::io::BufWriter::new(std::io::stdout());
    let mut prompt_count = 0_u64;
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let Ok(message) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let method = message
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let id = message.get("id").cloned();
        let response = match (method, id) {
            ("initialize", Some(id)) => Some(serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "protocolVersion": 1,
                    "agentCapabilities": {},
                    "agentInfo": { "name": "ACP fixture", "version": "1" }
                }
            })),
            ("session/new", Some(id)) => Some(serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "sessionId": "fixture-session"
                }
            })),
            ("session/prompt", Some(id)) => {
                prompt_count += 1;
                let text = match prompt_count {
                    1 => {
                        let prompt = message
                            .pointer("/params/prompt")
                            .and_then(serde_json::Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(|block| {
                                block.get("text").and_then(serde_json::Value::as_str)
                            })
                            .collect::<Vec<_>>()
                            .join("\n");
                        format!("acp done\nbackend={selected_model}\n{prompt}")
                    }
                    _ => "acp follow-up done".to_string(),
                };
                let notification = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": "fixture-session",
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": { "type": "text", "text": text }
                        }
                    }
                });
                writeln!(stdout, "{notification}").expect("write ACP fixture notification");
                Some(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": { "stopReason": "end_turn" }
                }))
            }
            _ => None,
        };
        if let Some(response) = response {
            writeln!(stdout, "{response}").expect("write ACP fixture response");
        }
        stdout.flush().expect("flush ACP fixture response");
    }
    std::process::exit(0);
}

#[cfg(not(target_os = "windows"))]
mod abort_tasks;
mod additional_context;
mod agent_execution;
mod agent_websocket;
mod agents_md;
mod apply_patch_cli;
#[cfg(not(target_os = "windows"))]
mod approvals;
mod audio_truncation;
mod auto_review;
mod catalog_permission_messages;
mod cli_stream;
mod client;
mod client_websockets;
mod cloud_config;
mod code_mode;
mod code_mode_elicitation;
mod codex_delegate;
mod collaboration_instructions;
mod compact;
mod compact_remote;
mod compact_remote_parity;
mod compact_resume_fork;
mod current_time_reminder;
mod cyber_exec_policy;
mod deprecation_notice;
mod exec;
mod exec_policy;
#[cfg(not(target_os = "windows"))]
mod extension_sandbox;
mod external_auth;
mod fork_thread;
mod git_enrichment;
#[cfg(not(target_os = "windows"))]
mod guardian_review;
#[cfg(not(target_os = "windows"))]
mod hooks;
#[cfg(not(target_os = "windows"))]
mod hooks_mcp;
mod image_rollout;
mod injected_models_cache;
mod items;
mod json_result;
mod live_cli;
mod mcp_auth_elicitation;
mod mcp_auth_refresh;
#[cfg(unix)]
mod mcp_refresh_cleanup;
mod mcp_startup_refresh_http_proxy;
mod mcp_tool_cache;
mod mcp_tool_exposure;
mod mcp_turn_metadata;
mod model_overrides;
mod model_runtime_selectors;
mod model_switching;
mod model_visible_layout;
mod models_cache_ttl;
mod models_etag_responses;
mod multi_agent_mode;
mod multi_agent_resume;
#[cfg(unix)]
mod multi_exec_server_sandbox;
mod network_approval;
mod openai_file_mcp;
mod otel;
mod override_updates;
mod pending_input;
mod permissions_messages;
mod personality;
mod plugins;
mod prompt_cache_key;
mod prompt_caching;
mod prompt_debug_tests;
mod quota_exceeded;
mod realtime_conversation;
mod realtime_initial_items;
mod remote_env;
mod remote_models;
mod request_compression;
#[cfg(not(target_os = "windows"))]
mod request_permissions;
#[cfg(not(target_os = "windows"))]
mod request_permissions_tool;
mod request_plugin_install;
mod request_user_input;
mod responses_api_proxy_headers;
mod responses_lite;
#[cfg(target_os = "linux")]
mod responses_system_proxy;
mod resume;
mod resume_warning;
mod retry_after;
mod review;
mod rmcp_client;
mod rollout_budget;
mod rollout_list_find;
mod safety_buffering;
mod safety_check_downgrade;
mod search_tool;
mod shell_command;
mod shell_serialization;
mod shell_snapshot;
mod skill_approval;
mod skills;
mod skills_extension;
mod spawn_agent_description;
mod sqlite_state;
mod stream_error_allows_next_turn;
mod stream_no_completed;
mod subagent_notifications;
mod token_budget;
mod tool_harness;
mod tool_lifecycle;
mod tool_parallelism;
mod tools;
mod truncation;
mod turn_input_submission;
mod turn_state;
mod unified_exec;
mod unified_exec_process_events;
#[cfg(unix)]
mod unified_exec_zsh_fork_approvals;
mod unstable_features_warning;
mod user_notification;
mod user_shell_cmd;
mod view_image;
mod web_search;
mod websocket_fallback;
mod window_headers;
#[cfg(target_os = "windows")]
mod windows_sandbox;
mod workspace_roots;
