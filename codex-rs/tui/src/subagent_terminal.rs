use crate::history_cell::HistoryCell;
use codex_app_server_protocol::SubAgentTerminalNotification;
use codex_app_server_protocol::SubAgentTerminalStatus;
use ratatui::prelude::Color;
use ratatui::prelude::Line;
use ratatui::prelude::Modifier;
use ratatui::prelude::Span;
use ratatui::prelude::Style;

#[derive(Debug)]
pub(crate) struct SubAgentTerminalHistoryCell {
    status: SubAgentTerminalStatus,
    identity: String,
    backend: Option<String>,
}

impl SubAgentTerminalHistoryCell {
    pub(crate) fn new(notification: SubAgentTerminalNotification) -> Self {
        Self {
            status: notification.status,
            identity: identity_for_notification(&notification),
            backend: notification.harness.as_deref().map(|harness| {
                crate::multi_agents::format_external_backend(harness, notification.model.as_deref())
            }),
        }
    }

    fn marker_and_label(&self) -> (&'static str, &'static str, Style) {
        match self.status {
            SubAgentTerminalStatus::Completed => {
                ("✓", "Subagent completed", Style::default().fg(Color::Green))
            }
            SubAgentTerminalStatus::Errored => {
                ("!", "Subagent failed", Style::default().fg(Color::Red))
            }
            SubAgentTerminalStatus::Interrupted => (
                "–",
                "Subagent interrupted",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::DIM),
            ),
        }
    }

    fn plain_text(&self) -> String {
        let (marker, label, _) = self.marker_and_label();
        let backend = self
            .backend
            .as_deref()
            .map(|backend| format!(" · {backend}"))
            .unwrap_or_default();
        format!("{marker} {label} · {}{backend}", self.identity)
    }
}

impl HistoryCell for SubAgentTerminalHistoryCell {
    fn display_lines(&self, _width: u16) -> Vec<Line<'static>> {
        let (marker, label, style) = self.marker_and_label();
        let mut spans = vec![
            Span::styled(marker.to_string(), style),
            Span::styled(format!(" {label}"), style),
            Span::styled(" · ", Style::default().add_modifier(Modifier::DIM)),
            Span::styled(self.identity.clone(), Style::default().fg(Color::Cyan)),
        ];
        if let Some(backend) = &self.backend {
            spans.push(Span::styled(
                " · ",
                Style::default().add_modifier(Modifier::DIM),
            ));
            spans.push(Span::styled(
                backend.clone(),
                Style::default().add_modifier(Modifier::DIM),
            ));
        }
        vec![Line::from(spans)]
    }

    fn raw_lines(&self) -> Vec<Line<'static>> {
        vec![Line::from(self.plain_text())]
    }
}

fn identity_for_notification(notification: &SubAgentTerminalNotification) -> String {
    let nickname = non_empty(notification.agent_nickname.as_deref());
    let role = non_empty(notification.agent_role.as_deref());
    match (nickname, role) {
        (Some(nickname), Some(role)) => format!("{nickname} [{role}]"),
        (Some(nickname), None) => nickname.to_string(),
        (None, Some(role)) => role.to_string(),
        (None, None) => notification
            .agent_path
            .as_deref()
            .and_then(|path| (!path.is_empty()).then_some(path))
            .map(ToString::to_string)
            .unwrap_or_else(|| notification.agent_thread_id.clone()),
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notification(status: SubAgentTerminalStatus) -> SubAgentTerminalNotification {
        SubAgentTerminalNotification {
            thread_id: "parent".to_string(),
            agent_thread_id: "child-thread".to_string(),
            agent_path: Some("/root/worker".to_string()),
            agent_nickname: Some("Luna".to_string()),
            agent_role: Some("reviewer".to_string()),
            harness: None,
            model: None,
            status,
        }
    }

    #[test]
    fn renders_all_terminal_statuses_without_body_text() {
        let expected = [
            (
                SubAgentTerminalStatus::Completed,
                "✓ Subagent completed · Luna [reviewer]",
            ),
            (
                SubAgentTerminalStatus::Errored,
                "! Subagent failed · Luna [reviewer]",
            ),
            (
                SubAgentTerminalStatus::Interrupted,
                "– Subagent interrupted · Luna [reviewer]",
            ),
        ];
        for (status, expected) in expected {
            let cell = SubAgentTerminalHistoryCell::new(notification(status));
            assert_eq!(cell.raw_lines()[0].to_string(), expected);
            assert!(!cell.raw_lines()[0].to_string().contains("parent"));
        }
    }

    #[test]
    fn falls_back_from_nickname_and_role_to_path_then_thread_id() {
        let mut notification = notification(SubAgentTerminalStatus::Completed);
        notification.agent_role = None;
        let cell = SubAgentTerminalHistoryCell::new(notification.clone());
        assert!(cell.raw_lines()[0].to_string().ends_with("Luna"));

        notification.agent_nickname = None;
        notification.agent_role = Some("reviewer".to_string());
        let cell = SubAgentTerminalHistoryCell::new(notification.clone());
        assert!(cell.raw_lines()[0].to_string().ends_with("reviewer"));

        notification.agent_nickname = None;
        notification.agent_role = None;
        let cell = SubAgentTerminalHistoryCell::new(notification.clone());
        assert!(cell.raw_lines()[0].to_string().ends_with("/root/worker"));

        notification.agent_path = None;
        let cell = SubAgentTerminalHistoryCell::new(notification);
        assert!(cell.raw_lines()[0].to_string().ends_with("child-thread"));
    }

    #[test]
    fn snapshots_terminal_statuses_and_identity_fallbacks() {
        let statuses = [
            SubAgentTerminalStatus::Completed,
            SubAgentTerminalStatus::Errored,
            SubAgentTerminalStatus::Interrupted,
        ];
        let status_lines = statuses
            .into_iter()
            .map(|status| {
                SubAgentTerminalHistoryCell::new(notification(status)).display_lines(80)[0]
                    .to_string()
            })
            .collect::<Vec<_>>();

        let mut path_notification = notification(SubAgentTerminalStatus::Completed);
        path_notification.agent_nickname = None;
        path_notification.agent_role = None;
        let path_line =
            SubAgentTerminalHistoryCell::new(path_notification).display_lines(80)[0].to_string();

        let mut thread_notification = notification(SubAgentTerminalStatus::Completed);
        thread_notification.agent_nickname = None;
        thread_notification.agent_role = None;
        thread_notification.agent_path = None;
        let thread_line =
            SubAgentTerminalHistoryCell::new(thread_notification).display_lines(80)[0].to_string();

        insta::assert_snapshot!(
            status_lines
                .into_iter()
                .chain([path_line, thread_line])
                .collect::<Vec<_>>()
                .join("\n"),
            @r###"✓ Subagent completed · Luna [reviewer]
! Subagent failed · Luna [reviewer]
– Subagent interrupted · Luna [reviewer]
✓ Subagent completed · /root/worker
✓ Subagent completed · child-thread"###
        );
    }

    #[test]
    fn snapshots_external_backend_identity_and_harness_default_model() {
        let mut selected = notification(SubAgentTerminalStatus::Completed);
        selected.harness = Some("cursor".to_string());
        selected.model = Some("cursor-grok-4.6-high".to_string());
        let mut defaulted = notification(SubAgentTerminalStatus::Errored);
        defaulted.harness = Some("grok-build".to_string());

        insta::assert_snapshot!(
            [selected, defaulted]
                .into_iter()
                .map(|notification| {
                    SubAgentTerminalHistoryCell::new(notification).display_lines(100)[0].to_string()
                })
                .collect::<Vec<_>>()
                .join("\n"),
            @r###"
✓ Subagent completed · Luna [reviewer] · cursor / cursor-grok-4.6-high
! Subagent failed · Luna [reviewer] · grok-build / default model
"###
        );
    }
}
