# Tutorials

Each tutorial runs one example application, page by page, so if you follow
the steps you have a working toy at the end. It is there to try things on, not
to deploy. The tutorials do not share their example, and nothing needs to be
left running from one to the next; each assumes you have done
[Up and running](../first_steps.html) and the tutorials before it. Their steps
work as they do there.

A tutorial page shows what to do and what you should see. What a piece of
Flezha is and how it works is on its topic page, which every tutorial links at
the first mention: the [topics](../building_blocks.html).

## The two tutorials

| Tutorial | What it runs |
|---|---|
| [The tradedesk](tradedesk/tradedesk.md) | A toy trader: an ETL stream that watches prices, and a workflow that places orders on a demo exchange |
| [The agentdesk](agentdesk/agentdesk.md) | An AI agent behind an HTTP service: sessions, streamed answers, calls between relays, several controllers and Kubernetes |

## Also useful

- [CLI](../cli.html): `flz` itself, how it reaches a controller and
  its output formats.
- [Troubleshooting](../runner_cannot_connect.html): from a symptom
  to its fix.
- [Local mode](../local_mode.html): run a relay's graph in your
  own process, with no controller.
