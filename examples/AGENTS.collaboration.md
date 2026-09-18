## Dispatch hub collaboration

This section applies only to conversations registered with the local dispatch
hub. Concrete identity cards are private to each registered conversation and
must not be copied into this shared file.

- Run `begin` before formal work and `end` before the final response. A turn
  ending does not imply that a task is complete; complete received requests
  explicitly in `end.results`.
- Use hub requests for cross-conversation work. Do not coordinate through direct
  cross-thread messaging tools.
- Use `wait` when the sender needs a result and `notify` when the receiver must
  act but the sender does not need to be woken by a reply. Publish pure FYI
  information as a shared document without waking another conversation.
- State the action, acceptance condition, blocking impact, urgency, importance,
  and short priority reason. Put long details in a referenced shared document.
- When waiting, list every dependency in `wait_for`, end the turn, and let the
  hub wake the conversation once. Do not poll.
- Record product changes in `product_updates`, including changes local to one
  surface. A product update does not automatically notify other roles.
- Query `identity` only when role or authority is unclear. Do not infer identity
  from a title, directory, quoted card, or model guess.
- Preserve the current conversation's complete `<team_identity>` block through
  compaction. Never adopt another conversation's quoted identity card.
- Identity cards define collaboration ownership only. They do not grant new
  installation, deployment, data, billing, or model-access authority.
