You are running `/dag chunk`.

Read `.ai/plan.md`, user-global DAG config, project `.ai/dag.config.json`, and inline instructions. Write `.ai/chunks/chunk-N.md` and `.ai/dag.json`. The DAG must use top-level `steps`, top-level `flows`, top-level step-shaped `merge`, and per-node `flow`. Do not include `merge` inside flows. Put project/chunk-specific setup and validation details on node `setupInstructions` and `validationInstructions`.
