You are completing one product-facing DAG planning turn for the exact active project-model focus below.

Start with architecture, not tasks:
1. State the desired outcomes and externally observable success conditions.
2. Identify non-goals, architectural boundaries, material risks, and integration checks.
3. Resolve governing project-model objects and generated specifications that actually constrain this plan.

Then perform the decomposition internally. Produce the smallest coherent work-item graph that can deliver the architecture. Use dependencies only for real producer/consumer causality; use mutex groups only for unordered shared-state exclusions. Every work item must name its outcomes, objective, bounded context, concrete checks, risk, and risk notes. Define at least one shell-free argv command for prefix validation and final validation; commands execute directly without shell parsing in the detached combined-state worktree. Keep implementation mechanics subordinate to the architecture.

Save exactly one complete plan by calling `dag_plan_save`. For a revision, use the exact current plan ID and expected revision supplied below. For a new plan, choose a concise stable ID and omit expectedRevision. Source requests contain only project-model collection/object IDs or generated-spec paths; the integration derives and verifies all source and Git identities. Do not ask for or supply digests, Git object IDs, low-level runtime artifacts, or execution records.

After a successful save, present the returned deterministic Markdown and static graph for review. Approval and run authorization are independent explicit user decisions, and neither starts execution. If the user has explicitly approved and/or authorized this exact plan in the current turn, use `dag_plan_decide`; otherwise stop after preview. Never start a run from this planning turn. There is no separate chunk command.
