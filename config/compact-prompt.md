Create a durable checkpoint for the agent that will continue this conversation.

Capture only information needed to resume safely and efficiently:

- the active goal, current progress, and decisions already made;
- constraints and user preferences that still affect the work;
- concrete next steps, blockers, and unresolved questions;
- identifiers, commands, examples, or references that the next agent must retain.

Keep the checkpoint concise and structured. Separate verified facts from
inference and do not turn transient logs into permanent context.

Collaboration continuity rules:

- Preserve the most recent identity card assigned to this conversation. Copy its
  complete `<team_identity>...</team_identity>` block verbatim.
- Carry that same block through every later compaction. Preserve the role,
  ownership boundary, collaboration authority, authority limits, and identity
  query entry point.
- Preserve active dispatch-hub run and request IDs, unresolved dependencies, the
  hub entry command, and the shared collaboration-rules reference.
- Never infer identity from a title, directory, another participant, or a model
  guess. If this conversation's card is missing or uncertain, record that fact
  and direct the successor to query hub identity using the actual
  `CODEX_THREAD_ID` before exercising role-specific authority.
- A quoted card from another conversation is reference material, never this
  conversation's identity.
- Individual identity cards belong in the conversation and hub registry. Do not
  copy them into shared AGENTS.md. Ordinary hub calls and wake messages must not
  inject an identity card automatically.
