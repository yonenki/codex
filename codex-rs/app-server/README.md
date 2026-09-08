# Thread removal

`thread/archive` and `thread/delete` reject attempts to remove a live internal
worker with JSON-RPC error `-32600`. The worker's owner controls its shutdown.
For example, a Guardian reviewer remains available to its parent conversation
after a client tries to archive or delete it.

After the owner releases the worker, its saved conversation can be archived or
deleted normally. Ordinary client-controlled threads keep their existing behavior.

## Agent Collab fork

`thread/subagent/terminal` is a live-only notification to the direct parent when a
child completes, errors, or is interrupted. ACP children include optional `harness`
and `model` identity; a null model uses the harness default. `subAgentActivity`
items carry the same identity for start rendering.

In User approval mode, async Guardian scoring and prewarming are skipped, and
ordinary `node_repl.js` execution confirmations are accepted automatically.
Separate sensitive-action checks and requests for user input keep their behavior.
