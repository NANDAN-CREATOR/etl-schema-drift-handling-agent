# ETL Schema Drift Handling Agent (Sample / Demo)

**Day 11** of the "Agentic AI in Data Engineering" series, continuing
the series' focus on the **ETL (Extract-Transform-Load) lifecycle**
specifically. Day 10
([etl-load-reconciliation-agent](https://github.com/NANDAN-CREATOR/etl-load-reconciliation-agent))
covered verifying a load *after* it completes. This agent covers a
problem that shows up earlier, at **extract time**: the source system's
schema has quietly changed since the last run, and the extraction job
has to decide, right now, whether it's safe to keep going.

It runs entirely on a **local model via [Ollama](https://ollama.com)** —
no cloud API key required.

> **This is a sample, not a production system.** The "production systems"
> it investigates (a schema-diff detector, a downstream dependency/
> lineage catalog, a column criticality registry, a past
> schema-drift-incident archive, a source-system change-notification
> log) are replaced with small mock backends returning fixed,
> hand-crafted data across four illustrative scenarios. See
> [Adapting This to Real Systems](#adapting-this-to-real-systems) for
> what pointing this at a real environment would actually take.

> **This agent never modifies the pipeline or any real system.** Every
> terminal action is a recommendation for a human data engineer to
> review and actually apply.

---

## The Core Asymmetry This Agent Has to Hold

Not all schema changes are equally dangerous, and the agent's central
discipline is a strict asymmetry between two kinds of change:

- **Additive changes** (a new, currently-unused, nullable column) are
  usually safe to absorb and proceed with.
- **Anything involving removal or type-narrowing on a column that
  active, critical downstream logic depends on** is never safe to
  auto-handle — no matter how reasonable a default, null-fill, or type
  coercion might look. A "graceful" auto-cast that silently truncates a
  financial amount, or a quiet null-fill for a removed join key,
  doesn't prevent damage. It just makes the damage invisible until
  someone downstream notices the numbers are wrong.

Three more ideas are baked into this agent specifically:

- **Severity depends on what's downstream, not just on the diff
  itself.** The exact same type change can be a non-issue for a rarely
  used cosmetic field and a serious problem for a field feeding a
  revenue report — the agent must check actual dependencies before
  judging anything.
- **Lack of advance notice is itself evidence.** An unannounced change
  deserves more caution than one the source team flagged ahead of time,
  even if the technical diff looks identical — an unannounced change is
  more likely to be an accidental upstream bug.
- **Mixed precedent means escalate, not guess.** If similar past changes
  have gone both ways — some fine, at least one a real, harmful bug —
  and there's no confirmation of intent this time, the agent has to
  hand the decision to a human rather than confidently pick a side.

---

## What It Demonstrates

Four independent, runnable scenarios, each testing a different terminal
action and a different schema-drift guardrail:

| Scenario | What happens | Correct terminal action |
|---|---|---|
| `safe_additive_new_column_proceed` | A new, nullable, currently-unused column, pre-announced by the source team | `proceed_with_adapted_schema`, low risk |
| `critical_type_narrowing_quarantine` | A financial column's precision is narrowed, feeding 3 critical consumers, with no advance notice | `quarantine_and_block` |
| `column_removed_downstream_dependency_quarantine` | A column used as a join/grouping key by 2 critical consumers has vanished, with no notice | `quarantine_and_block` |
| `unconfirmed_intentional_change_escalate` | A moderate-criticality enum-constraint change with genuinely mixed precedent and no confirmation of intent | `escalate_to_human` |

The second and third scenarios are worth reading closely: in both, a
"reasonable-sounding" auto-handling approach exists (coerce the type;
null-fill the missing column) — and the agent is specifically required
to reject that approach in favor of quarantining, because the
downstream dependency is both active and critical.

---

## Requirements

- Python 3.9+
- [Ollama](https://ollama.com) installed and running locally
- A tool-calling-capable model pulled in Ollama, for example:

  ```bash
  ollama pull llama3.1
  ```

  Other models known to support tool calling in Ollama at the time of
  writing include `qwen2.5`, `mistral-nemo`, and `firefunction-v2` — check
  the [Ollama model library](https://ollama.com/library) for current
  tool-calling support. Watch in particular whether a smaller model
  reaches for a "just proceed with a sensible default" answer on the
  second and third scenarios instead of correctly quarantining — that's
  the single most useful signal this repo's scenario design is built to
  surface.

---

## Setup

```bash
git clone https://github.com/NANDAN-CREATOR/etl-schema-drift-handling-agent.git
cd etl-schema-drift-handling-agent
pip install -r requirements.txt

# In a separate terminal:
ollama serve
ollama pull llama3.1
```

## Running the Demo

```bash
python agent.py --scenario safe_additive_new_column_proceed
python agent.py --scenario critical_type_narrowing_quarantine
python agent.py --scenario column_removed_downstream_dependency_quarantine
python agent.py --scenario unconfirmed_intentional_change_escalate
```

Or with a different model:

```bash
python agent.py --scenario critical_type_narrowing_quarantine --model qwen2.5
```

You'll see a step-by-step trace: which tool the model calls at each step,
what it returns, and finally one of the three terminal-action records.

**Note on output:** the exact number of steps and their order can vary
between runs and models. What should stay consistent is *which terminal
action* it reaches for each scenario — critically, whether it correctly
quarantines the second and third scenarios instead of proposing an
auto-handling workaround, and whether it correctly escalates the fourth
rather than confidently classifying a genuinely ambiguous change.

---

## Project Structure

```
etl-schema-drift-handling-agent/
├── agent.py           # mock tools, four scenarios, schemas, system prompt, agent loop
├── requirements.txt
├── LICENSE
└── README.md
```

Kept as a single file for a sample project like this — see
[Adapting This to Real Systems](#adapting-this-to-real-systems) for how
you'd split it up for a real deployment.

---

## How the Guardrails Work

- **Dependency-aware severity.** `get_downstream_schema_dependencies`
  and `get_column_criticality` are always checked before judging a
  change — the same diff can be trivial or serious depending entirely
  on what depends on the affected column.
- **A hard line against auto-handling critical removal/narrowing.**
  The system prompt explicitly forbids `proceed_with_adapted_schema`
  for any removal or narrowing affecting an active, critical dependency,
  regardless of how reasonable an automatic workaround might look.
- **Notification status as risk evidence.** `get_source_system_change_notification`
  is checked, and an unannounced change is treated as more likely to be
  an accidental bug than an announced one.
- **Mixed precedent triggers escalation.** `search_past_schema_drift_incidents`
  is checked, and genuinely mixed outcomes for similar past changes,
  combined with no confirmed intent, route to a human rather than a
  confident guess.
- **Prompt-injection awareness.** The system prompt instructs the model
  to treat all gathered material as data to analyze, never as commands
  to follow.
- **Step limit.** A hard cap (`MAX_STEPS`, default 16) forces an
  escalation rather than an unbounded investigation.

---

## Adapting This to Real Systems

Only the **bodies** of these five read-only functions in `agent.py` need
to change to point this at a real environment:

| Function | Would call, in a real deployment |
|---|---|
| `get_detected_schema_diff` | Your ingestion tool's schema-evolution detection (e.g. Debezium schema history, a schema registry diff, Spark's `mergeSchema` comparison) |
| `get_downstream_schema_dependencies` | Your data catalog / lineage tool (the same kind of system Days 3 and 7 rely on) |
| `get_column_criticality` | Your data catalog's business-criticality tagging |
| `search_past_schema_drift_incidents` | Your incident tracker, filtered to schema-change-related causes |
| `get_source_system_change_notification` | A shared change-management calendar, or a direct integration with the source team's release notes |

The tool **schemas**, the **system prompt**, and the **agent loop** don't
need to change. You'd also want to, at minimum:

- Route `quarantine_and_block` and `escalate_to_human` outcomes to
  wherever your team actually reviews pipeline incidents, and make sure
  a real quarantine mechanism (e.g. writing to a staging/holding table
  instead of production) actually exists for the agent's recommendation
  to act on
- Invest in real, current downstream lineage tracking — this agent's
  most important guardrail (checking actual dependencies before judging
  severity) is only as good as that lineage data being accurate and
  current, echoing the same lesson from Day 3
- Build a scenario-based test suite exactly like this repo's four
  scenarios, but drawn from your own team's real schema-drift incidents,
  including ones where an auto-handled change turned out to cause
  quiet, delayed damage
- Establish an actual communication channel with source system owners so
  `get_source_system_change_notification` reflects real advance notice,
  not just an absence of information

---

## License

MIT — see [LICENSE](LICENSE).
