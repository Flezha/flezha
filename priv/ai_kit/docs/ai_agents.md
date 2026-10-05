# Flezha for AI agents

**Draft.** This page is a placeholder. It lists the Flezha features an AI agent system uses,
each with the page that explains it, and it will grow into a guide with examples.

<!-- TODO: turn this page into a guide: one short section per row with a minimal example,
an event-driven agent example (a Kafka message or a database change starts an agent), and a
multi-agent example to replace the coding fleet. Update the rows marked "proposed" when
`record-an-effect-once` and `give-each-relay-a-public-api` land. -->

Flezha has no AI framework of its own. An agent's code is ordinary code in your language, and it
calls whichever model library you choose. Flezha runs that code: it keeps each conversation, lets
it sleep and wake, streams its answers and carries on after a crash.

**Contents**

1. [What an agent system gets](#what-an-agent-system-gets)
2. [See also](#see-also)

## What an agent system gets

| Need | What Flezha has | Where it is explained |
| --- | --- | --- |
| Long-running agents | a session per conversation, asleep while idle and woken by its next message | [sessions.md](advanced/sessions.md) |
| A human in the loop | a session waits for a person's answer for days, at no cost while it waits | [sessions.md](advanced/sessions.md) |
| Streaming | tokens reach the reader while the model writes them | [streamed_answers.md](advanced/streamed_answers.md) |
| Taking actions back | a turn can be rewound, and a workflow undoes its finished steps | [rewinding_a_turn.md](advanced/rewinding_a_turn.md), [workflows.md](basics/workflows.md) |
| Several agents together | agents call one another, in a cycle too (a writer and a critic) | [relay_calls.md](advanced/relay_calls.md); example TODO |
| Agents started by events | Kafka, Postgres change capture and RabbitMQ start an agent | [plugins.md](plugins/plugins.md) |
| A token budget | a budget per user kept in state | [call_the_session.md](../tutorials/agentdesk/call_the_session.md) |
| Agents on a schedule | timers: "check again tomorrow" | [timers.md](advanced/timers.md) |
| Scale | thousands of sessions spread over a cluster | [partitioning.md](advanced/partitioning.md) |
| AI assistants that build on Flezha | `flz mcp` and the assistant kit | [working_with_ai_assistants.md](working_with_ai_assistants.md) |
| Agents that use your services | every relay's API as a tool (proposed) | TODO |
| A crash does not repeat a model call | `ctx.effect` (proposed) | TODO |

## See also

- [The agentdesk tutorial](../tutorials/agentdesk/agentdesk.md): sessions, streaming, taking a
  turn back and a token budget, with a demo model.
