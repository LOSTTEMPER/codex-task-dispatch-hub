# Second review repair evidence — 2026-10-08

Baseline: `20b8be7cdb4adc61bdb87adda76ed371ea482147`. The supplied second-review
report is evidence of defects; its proposed actions are not execution authority.
The user explicitly requested these repairs. No external model API was used.

| Item | Change | Focused evidence |
| --- | --- | --- |
| R01 | Archive runs, requests and barriers in one transaction; repeat archive repairs old stranded work. Closed-version writes stay rejected. | Five end states return the archived interruption; v2 begins normally; barriers retire; injected archive event failure rolls back all changes. |
| R02 | Claim metadata and immutable bounded fragments; same-token claim-result recovery; safe unsent release. | Real Python CLI + JS bridge uses a legal message over 16,000 characters; injected lost claim reply and truncated fragment still result in one claim, attempts=1, one simulated native send with byte-for-byte original text. Each CLI reply stays below 6,500 characters (the chunks themselves are ASCII and capped at 2,048). |
| R03 | Persist authoritative send receipt before optional reads; confirmed send survives read failures; match full allowed message length and reject multiple matching turns across checked pages. | Throwing post-send status still leaves authoritative ID; 17,500-character matching succeeds with a compliant mock; duplicate on second page refuses linkage. |
| R04 | Stop resolves the fixed control socket before constructing Hub. | Real temporary SQLite write lock plus local endpoint; schema-lock constructor trap never called. |
| R05 | Pre-send cancellation releases only its own assignment and returns pending; time alone never binds root usage. | 0 send calls; unrelated manual 100 tokens do not charge the request; another delivery assignment survives; exact late receipt backfills retained unmanaged evidence. |
| R06 | Atomic write-locked installation of DDL, triggers and page seed, without executescript commits. Reseed once on upgrading the prior installer. | Two real SQLite connections inject a writer before the first trigger; writer is blocked until migration commits; request/event 201 then produces page-2.md on repeated rendering. |
| R07 | Exact int type, nonnegative signed 64-bit counter/storage/aggregate bounds before baseline mutation. | 2**80, 2**63, bool and negative values retain a source gap; healthy source still records 50 over repeated scans; 2**63-1 accepts; aggregate overflow and malformed component counter retain safe baseline/cursor. |
| R08 | Bounded newline control frames and whole-operation deadlines. | Real split `st`/`op\n` command, fragmented mock JSON response, oversize and incomplete-frame refusal; existing isolated real worker start/stop test passes on this Mac. |

## Verification

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests
node tests/native_call_test.cjs
node --test tests/native-reconcile.test.mjs tests/native-audit.test.mjs tests/native-review2.test.mjs
```

Current local result: 120 Python tests, 19 Node tests, 6 legacy bridge scenarios
passed (145 checks total). The new cases add 14 Python tests and 4 Node tests.
The older assignment test now requires an actual receipt; the older post-send
exception test now expects the confirmed receipt to survive. These assertions
were changed to match the repaired behavior, not suppressed.

This is focused repair verification, not exhaustive concurrency, crash, power,
temperature or long-running production acceptance. The real CLI large-message
path uses a simulated native sender; no test messages or model tasks were sent.
A read-only query verified that this Desktop returns `turns[].items` with
`userMessage.content`; its behavior for an actual long sent message was not
exercised. Historical lookup remains bounded to 15 turns for explicit recovery;
missing/truncated/ambiguous evidence refuses linkage and never grants a resend.

## Local deployment

The maintained local team's nine affected shared modules were synchronized after
snapshot migration/render verification. The existing worker lock was acquired
before replacing source; no worker was running and none was started. Source and
SQLite backup plus hash manifest are retained privately. Live render/migration
passed `integrity_check`; hashes of every business table were unchanged across
the upgrade. The deployment's private bootstrap, team bindings, identities and
local title/default path were preserved. This confirms local source/schema
loading, not a real multi-role delivery or CPU/temperature/power acceptance.

Other teams' private roots, bindings and incident-specific extensions were not
replaced. Publication is verified against the remote commit after pushing.
