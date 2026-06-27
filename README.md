# AGY-QQ Bridge: Project Summary

*Generated: 2026-06-27 | Runtime: 48h+ | Iterations: 7 versions (v4→v8.2)*

---

## 1. What This System Is

A persistent QQ-to-CLI bridge system that connects real-time chat input to an autonomous CLI agent (AGY), preserving session memory and execution continuity.

---

## 2. What We Built

| Layer | Component | Detail |
|-------|-----------|--------|
| Input | QQ message ingestion | WebSocket gateway, C2C + group support |
| Bridge | Python bridge (891 lines) | TCP socket / tmux send-keys / transcript reader |
| Runtime | AGY persistent session | tmux session, 48h+ uptime |
| Output | transcript.jsonl capture | PLANNER_RESPONSE structured extraction |
| Approval | Interactive TUI handling | capture-pane fallback + QQ button cards |
| Event log | Full audit trail | Event Ledger, Master Timeline, system_state.json |

---

## 3. What Works

- Long-running AGY sessions (48h+ continuous, PID 4046885)
- Memory persistence across sessions (AGY brain transcript)
- Response capture via transcript.jsonl (structured, no history leakage)
- Message routing: QQ → bridge → tmux → AGY (stable)
- Event traceability: all failures recorded in Event Ledger (30+ events)
- Approval button flow: detection → QQ card → user click → keystroke (functional)
- Code base: versioned in `github.com/zz327455573/agent_qqbot_bridge`

---

## 4. What Breaks

*Facts only — no explanation.*

- capture-pane unreliable as data source (terminal content is mixed-output)
- Approval TUI not machine-readable (visual affordance only, no structured event)
- No intermediate state exposure (transcript only records `status=DONE`)
- Only final state visible in logs (no PENDING/WAITING/APPROVAL entries)
- Occasional UI contamination in output (AGY discussion about approvals triggers false detection)

---

## 5. Key Insight

> CLI agents do not expose structured intermediate execution states, only final completion states.

---

## 6. Conclusion

> This system demonstrates both the feasibility of external orchestration of CLI agents and the structural limitation in their observability model.

---

*Full evidence: /root/AGY_BRIDGE_SNAPSHOT/ (472KB, 20 files)*