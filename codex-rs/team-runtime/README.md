# Team runtime operations

`team.advance_team` records a caller-supplied verdict and evidence, selects its
declared transition, and starts the next nonterminal node in one transaction.
It starts the current node when needed. Terminal nodes still require an explicit
`end_team` decision. Existing granular operations remain available.

The operation takes the same result fields as `record_team_result`, plus the
transition's `deviation_reason`. It checks the expected revision, open lifecycle,
active agents, external waits, and declared transition before applying changes.
It never infers an approval or skips a graph node. Events, evidence, metrics, and
the outbox keep their existing format; all events and the snapshot commit together.
Once accepted, the transaction settles even if the tool caller cancels. Read the
current status before retrying after a lost response.

Routine Team operations return a compact progress record. Operations that change
nodes also return the new guide. `get_team_status` retains the full diagnostic
view, including graph identity and available operations. Agent-spawn results and
Team listings no longer repeat the full guide.

`collaboration.list_agents` returns lifecycle states by default, without repeating
completed reports or error text. Use `detail: "full"` with a task `path_prefix` to
retrieve those details. Pages default to 20 entries (maximum 100); `total` and
`next_offset` make omitted entries explicit. Terminal notifications and saved
Agent results are unchanged.

Focused verification covers durable advancement and rollback, stale revisions,
active reviewers, explicit result retrieval, and the model-facing Team tool path.
