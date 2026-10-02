import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { Repository } from "@taskasaur/server-host";
import type { Value } from "@taskasaur/platform/field-types";
import type { Principal } from "@taskasaur/platform/plugin-sdk";
import { validateGraph } from "@taskasaur/platform/core/workflows";
import { invariant } from "@taskasaur/platform/core/errors";
export async function dispatchWorkflow(
  repo: Repository,
  principal: Principal,
  id: string,
  targetDeviceId: string,
  options: { operationId?: string; input?: Value } = {},
) {
  return repo.db.transaction((tx) =>
    dispatchWithin(new Repository(tx), principal, id, targetDeviceId, options),
  );
}
async function dispatchWithin(
  repo: Repository,
  principal: Principal,
  id: string,
  targetDeviceId: string,
  options: { operationId?: string; input?: Value },
) {
  await repo.db.query(
    "SELECT id FROM taskasaur.workspaces WHERE id=$1 FOR UPDATE",
    [principal.workspaceId],
  );
  await repo.requirePlugin(principal, "automation-runtime");
  const workflow = await repo.get(principal, id);
  await repo.authorize(principal, id, true);
  invariant(
    workflow.collection === "workflows",
    "VALIDATION_FAILED",
    "Expected a workflow",
  );
  const device = await repo.get(principal, targetDeviceId);
  invariant(
    device.collection === "devices" &&
      !device.data.revoked &&
      Array.isArray(device.data.capabilities) &&
      device.data.capabilities.includes("automation.execute"),
    "CAPABILITY_UNSUPPORTED",
    "This device cannot execute workflows",
  );
  invariant(
    device.ownerId === principal.userId,
    "PERMISSION_DENIED",
    "Device execution permission is required",
  );
  const version = Number(workflow.data.published_version);
  invariant(
    Number.isInteger(version) && version > 0,
    "WORKFLOW_UNPUBLISHED",
    "Publish this workflow before running it",
  );
  const online = Date.now() - Date.parse(String(device.data.last_seen)) < 45000;
  invariant(
    online || workflow.data.offline_policy === "waitForDevice",
    "DEVICE_OFFLINE",
    "Selected device is offline",
  );
  const timeout = Number(workflow.data.dispatch_timeout_seconds);
  if (workflow.data.offline_policy === "waitForDevice")
    invariant(
      timeout > 0 && timeout <= 86400,
      "VALIDATION_FAILED",
      "Waiting requires a bounded dispatch timeout",
    );
  const pinned = (
    await repo.db.query<{ graph: unknown; trusted_code: boolean }>(
      "SELECT graph,trusted_code FROM taskasaur.workflow_versions WHERE workflow_id=$1 AND version=$2",
      [id, version],
    )
  ).rows[0];
  invariant(
    pinned,
    "WORKFLOW_UNPUBLISHED",
    "Synchronize and publish this workflow before running it",
  );
  const graph = validateGraph(pinned.graph);
  invariant(
    !pinned.trusted_code || workflow.data.allow_trusted_code === true,
    "PERMISSION_DENIED",
    "Trusted TypeScript execution is no longer enabled",
  );
  const runId = options.operationId ?? randomUUID(),
    execution = {
      id: runId,
      interpreterVersion: 2 as const,
      targetDeviceId,
      graph,
      input: options.input ?? {},
      allowTrustedCode: pinned.trusted_code,
    };
  const prior = (
    await repo.db.query<{
      execution: unknown;
      workspace_id: string;
      user_id: string;
    }>(
      "SELECT execution,workspace_id,user_id FROM taskasaur.dispatches WHERE run_id=$1",
      [runId],
    )
  ).rows[0];
  if (prior) {
    invariant(
      prior.workspace_id === principal.workspaceId &&
        prior.user_id === principal.userId &&
        isDeepStrictEqual(prior.execution, execution),
      "IDEMPOTENCY_CONFLICT",
      "Run operation ID was already used for a different request",
    );
    return repo.get(principal, runId);
  }
  const run = await repo.mutate(principal, {
    id: randomUUID(),
    resourceId: runId,
    pluginId: "automation-runtime",
    collection: "workflow_runs",
    operation: "put",
    baseRevision: 0,
    createdAt: new Date().toISOString(),
    data: {
      workflow_id: id,
      target_device_id: targetDeviceId,
      workflow_version: version,
      status: "queued",
      input: execution.input,
    },
  });
  await repo.db.query(
    "INSERT INTO taskasaur.dispatches(run_id,device_id,workspace_id,user_id,execution,expires_at) VALUES($1,$2,$3,$4,$5,$6)",
    [
      runId,
      targetDeviceId,
      principal.workspaceId,
      principal.userId,
      JSON.stringify(execution),
      new Date(
        Date.now() +
          (workflow.data.offline_policy === "waitForDevice" ? timeout : 60) *
            1000,
      ).toISOString(),
    ],
  );
  return run;
}
