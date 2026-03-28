# Layer 6 Transport Improvement -- Task Manifest

## Execution Summary
- Total tasks: 10
- Parallel groups: 5
- Estimated agents: 10 (plus 1 de-sloppify)

## Dependency Graph

| Task | Depends On | Model | Isolation | Group |
|------|-----------|-------|-----------|-------|
| 001-enrichment-config | none | sonnet | worktree | 1 |
| 002-data-joiner | none | sonnet | worktree | 1 |
| 003-prompt-builder | none | sonnet | worktree | 1 |
| 004-update-layer6-config | none | sonnet | worktree | 1 |
| 005-llm-client | 001 | sonnet | worktree | 2 |
| 006-validator | 001 | sonnet | worktree | 2 |
| 007-transport-calculation | 004 | opus | worktree | 2 |
| 008-processing-calculator | 004 | opus | worktree | 2 |
| 009-orchestrator-and-script | 002,003,005,006 | opus | worktree | 3 |
| 010-integration | all | opus | worktree | 4 |

## Execution Order
- **Group 1** (parallel): 001, 002, 003, 004 -- independent config, data, prompt, and schema setup
- **Group 2** (parallel): 005, 006, 007, 008 -- client, validator, and calculation modifications
- **Group 3**: 009 -- orchestrator wiring (depends on all enrichment components)
- **Group 4**: 010 -- integration verification across both phases
- **Group 5**: de-sloppify pass on all modified files
