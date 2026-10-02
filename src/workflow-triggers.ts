import { createHash, randomBytes, randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { Repository } from "@taskasaur/server-host";
import { dispatchWorkflow } from "./dispatch";
import { operationUuid, type Dispatch } from "@taskasaur/server-host";
import { CoreError, invariant } from "@taskasaur/platform/core/errors";
import { allNodes, validateGraph } from "@taskasaur/platform/core/workflows";
import type {
  Principal,
  ResourceRecord,
  PluginEvent,
} from "@taskasaur/platform/plugin-sdk";
import type { Value } from "@taskasaur/platform/field-types";

export type WorkflowSignal = { id: string; name: string; data: Value };
export async function queueWorkflowSignal(
  repo: Repository,
  principal: Principal,
  runId: string,
  name: string,
  data: Value,
  id: string,
) {
  await repo.requirePlugin(principal, "automation-runtime");
  await repo.authorize(principal, runId, true);
  const dispatch = (
    await repo.db.query<Dispatch>(
      "SELECT * FROM taskasaur.dispatches WHERE run_id=$1 AND workspace_id=$2",
      [runId, principal.workspaceId],
    )
  ).rows[0];
  invariant(
    dispatch &&
      ["queued", "accepted"].includes(dispatch.status) &&
      !dispatch.cancel_requested,
    "INVALID_RUN_STATE",
    "Run is no longer active",
  );
  invariant(
    allNodes(validateGraph(dispatch.execution.graph)).some(
      (node) => node.type === "signal" && node.config.name === name,
    ),
    "VALIDATION_FAILED",
    "This run does not declare that signal",
  );
  return repo.db.transaction(async (tx) => {
    await tx.query(
      "INSERT INTO taskasaur.workflow_signals(id,run_id,name,data) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO NOTHING",
      [id, runId, name, JSON.stringify(data)],
    );
    const prior = (
      await tx.query<{ run_id: string; name: string; data: Value }>(
        "SELECT run_id,name,data FROM taskasaur.workflow_signals WHERE id=$1",
        [id],
      )
    ).rows[0];
    invariant(
      prior.run_id === runId &&
        prior.name === name &&
        isDeepStrictEqual(prior.data, data),
      "IDEMPOTENCY_CONFLICT",
      "Signal operation ID already has another payload",
    );
    return { id };
  });
}
export async function rotateWorkflowHook(
  repo: Repository,
  principal: Principal,
  id: string,
) {
  await repo.requirePlugin(principal, "automation-runtime");
  await repo.authorize(principal, id, true);
  const workflow = await repo.get(principal, id);
  invariant(
    workflow.collection === "workflows" &&
      workflow.data.trigger_kind === "webhook",
    "VALIDATION_FAILED",
    "Select the webhook trigger first",
  );
  const token = randomBytes(32).toString("base64url");
  await repo.db.query(
    "INSERT INTO taskasaur.workflow_hooks(workflow_id,token_hash) VALUES($1,$2) ON CONFLICT(workflow_id) DO UPDATE SET token_hash=EXCLUDED.token_hash,created_at=now()",
    [id, createHash("sha256").update(token).digest("hex")],
  );
  return { path: "/api/automation/hooks/" + id, token };
}
export async function receiveWorkflowHook(
  repo: Repository,
  id: string,
  token: string,
  operationId: string,
  input: Value,
) {
  const row = (
    await repo.db.query<{ workspace_id: string; owner_id: string }>(
      "SELECT r.workspace_id,r.owner_id FROM taskasaur.workflow_hooks h JOIN taskasaur.resources r ON r.id=h.workflow_id WHERE h.workflow_id=$1 AND h.token_hash=$2 AND r.deleted_at IS NULL",
      [id, createHash("sha256").update(token).digest("hex")],
    )
  ).rows[0];
  invariant(row, "PERMISSION_DENIED", "Webhook credential is invalid");
  const principal: Principal = {
    workspaceId: row.workspace_id,
    userId: row.owner_id,
    pluginId: "automation-runtime",
    permissions: [],
  };
  const workflow = await repo.get(principal, id);
  invariant(
    workflow.data.enabled && workflow.data.trigger_kind === "webhook",
    "FEATURE_DISABLED",
    "Webhook workflow is disabled",
  );
  return dispatchWorkflow(
    repo,
    principal,
    id,
    String(workflow.data.target_device_id),
    { operationId: operationUuid(id + ":webhook:" + operationId), input },
  );
}

export async function processWorkflowTriggers(
  repo: Repository,
  workspaceId: string,
  now = new Date(),
) {
  const rows = await repo.db.query<{ id: string; owner_id: string }>(
    "SELECT r.id,r.owner_id FROM taskasaur.resources r JOIN taskasaur.p_workflows w ON w.id=r.id JOIN taskasaur.plugins p ON p.workspace_id=r.workspace_id AND p.id='automation-runtime' AND p.enabled WHERE r.workspace_id=$1 AND r.deleted_at IS NULL AND w.enabled AND w.published_version IS NOT NULL AND w.trigger_kind IN ('schedule','event') ORDER BY r.created_at LIMIT 100",
    [workspaceId],
  );
  for (const row of rows.rows) {
    const principal: Principal = {
      workspaceId,
      userId: row.owner_id,
      pluginId: "automation-runtime",
      permissions: [],
    };
    try {
      await repo.db.transaction(async (tx) => {
        await tx.query(
          "SELECT id FROM taskasaur.workspaces WHERE id=$1 FOR UPDATE",
          [workspaceId],
        );
        const scoped = new Repository(tx),
          workflow = await scoped.get(principal, row.id),
          data = workflow.data;
        const configuration = createHash("sha256")
          .update(
            JSON.stringify([
              data.published_version,
              data.target_device_id,
              data.trigger_kind,
              data.schedule_seconds,
              data.schedule_start,
              data.catch_up,
              data.event_type,
            ]),
          )
          .digest("hex");
        const cursor = (
          await tx.query<{
            configuration: string;
            slot: string;
            event_cursor: string;
          }>("SELECT * FROM taskasaur.workflow_triggers WHERE workflow_id=$1", [
            row.id,
          ])
        ).rows[0];
        let slot =
            cursor?.configuration === configuration ? Number(cursor.slot) : -1,
          eventCursor =
            cursor?.configuration === configuration ? cursor.event_cursor : "0";
        if (data.trigger_kind === "schedule") {
          const interval = Number(data.schedule_seconds) * 1000,
            start = Date.parse(
              String(data.schedule_start ?? workflow.createdAt),
            );
          invariant(
            interval >= 10000 && Number.isFinite(start),
            "VALIDATION_FAILED",
            "Schedule requires an interval and valid start",
          );
          const current = Math.floor((now.getTime() - start) / interval);
          if (current >= 0 && current > slot) {
            const first =
              data.catch_up === "all"
                ? Math.max(
                    slot + 1,
                    current - Math.floor((7 * 86400000) / interval),
                  )
                : current;
            const last = Math.min(current, first + 99);
            for (let occurrence = first; occurrence <= last; occurrence++) {
              if (
                data.catch_up === "skip" &&
                now.getTime() - (start + occurrence * interval) > 10000
              )
                continue;
              await dispatchWorkflow(
                scoped,
                principal,
                row.id,
                String(data.target_device_id),
                {
                  operationId: operationUuid(
                    row.id + ":" + configuration + ":schedule:" + occurrence,
                  ),
                  input: {
                    trigger: "schedule",
                    scheduledAt: new Date(
                      start + occurrence * interval,
                    ).toISOString(),
                  },
                },
              );
            }
            slot = last;
          }
        } else {
          invariant(
            typeof data.event_type === "string" &&
              data.event_type.startsWith("taskasaur."),
            "VALIDATION_FAILED",
            "Choose a complete committed event type",
          );
          // A new configuration starts with future committed events; it cannot replay years of history by surprise.
          if (!cursor || cursor.configuration !== configuration)
            eventCursor = (
              await tx.query<{ cursor: string }>(
                "SELECT COALESCE(max(sequence),0)::text AS cursor FROM taskasaur.plugin_events WHERE workspace_id=$1",
                [workspaceId],
              )
            ).rows[0].cursor;
          const events = await tx.query<{
            sequence: string;
            event: PluginEvent;
          }>(
            "SELECT sequence::text,event FROM taskasaur.plugin_events WHERE workspace_id=$1 AND sequence>$2 AND event->>'type'=$3 ORDER BY sequence LIMIT 100",
            [workspaceId, eventCursor, data.event_type],
          );
          for (const event of events.rows) {
            try {
              await scoped.authorize(principal, event.event.data.resourceId);
            } catch (error) {
              if (
                error instanceof CoreError &&
                ["PERMISSION_DENIED", "NOT_FOUND"].includes(error.kind)
              ) {
                eventCursor = event.sequence;
                continue;
              }
              throw error;
            }
            await dispatchWorkflow(
              scoped,
              principal,
              row.id,
              String(data.target_device_id),
              {
                operationId: operationUuid(
                  row.id + ":" + configuration + ":event:" + event.event.id,
                ),
                input: {
                  trigger: "event",
                  event: event.event as unknown as Value,
                },
              },
            );
            eventCursor = event.sequence;
          }
        }
        await tx.query(
          "INSERT INTO taskasaur.workflow_triggers(workflow_id,configuration,slot,event_cursor,error) VALUES($1,$2,$3,$4,NULL) ON CONFLICT(workflow_id) DO UPDATE SET configuration=EXCLUDED.configuration,slot=EXCLUDED.slot,event_cursor=EXCLUDED.event_cursor,error=NULL",
          [row.id, configuration, slot, eventCursor],
        );
      });
    } catch (error) {
      await repo.db.query(
        "INSERT INTO taskasaur.workflow_triggers(workflow_id,error) VALUES($1,$2) ON CONFLICT(workflow_id) DO UPDATE SET error=EXCLUDED.error",
        [row.id, error instanceof CoreError ? error.kind : "TRIGGER_FAILED"],
      );
    }
  }
}
