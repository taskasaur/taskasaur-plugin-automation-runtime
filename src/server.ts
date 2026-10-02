import { z } from "zod";
import { invariant, CoreError } from "@taskasaur/platform/core/errors";
import type { Value } from "@taskasaur/platform/field-types";
import type { Principal } from "@taskasaur/platform/plugin-sdk";
import {
  Repository,
  CredentialBroker,
  FileService,
  DeviceService,
  JobService,
  routerFor,
} from "@taskasaur/server-host";
import { dispatchWorkflow } from "./dispatch";
import {
  queueWorkflowSignal,
  rotateWorkflowHook,
  receiveWorkflowHook,
  processWorkflowTriggers,
} from "./workflow-triggers";
import { validateGraph, allNodes } from "@taskasaur/platform/core/workflows";
import { compileTypeScript } from "@taskasaur/platform/automation/typescript";

export function createBackend(core: any) {
  return {
    async http(request: Request, route: string, input: any) {
      const { repo, principal } = input,
        path = route,
        url = new URL(request.url),
        workspaceId = principal?.workspaceId;
      const broker = () =>
        new CredentialBroker(repo, process.env.CREDENTIAL_ENCRYPTION_KEY ?? "");
      const services = () => ({ repo });
      if (path.startsWith("automation/hooks/") && request.method === "POST") {
        const id = z.string().uuid().parse(path.split("/")[2]);
        const operationId = z
          .string()
          .min(1)
          .max(200)
          .parse(request.headers.get("Idempotency-Key"));
        const reader = request.body?.getReader();
        invariant(reader, "VALIDATION_FAILED", "Webhook body is required");
        const chunks: Uint8Array[] = [];
        let length = 0;
        try {
          for (;;) {
            const chunk = await reader.read();
            if (chunk.done) break;
            length += chunk.value.length;
            invariant(
              length <= 1024 * 1024,
              "PAYLOAD_TOO_LARGE",
              "Webhook exceeds 1 MB",
            );
            chunks.push(chunk.value);
          }
        } finally {
          await reader.cancel();
        }
        const input = JSON.parse(
          Buffer.concat(chunks).toString("utf8"),
        ) as Value;
        const token =
          request.headers.get("Authorization")?.replace(/^Bearer /, "") ?? "";
        return Response.json(
          await receiveWorkflowHook(
            services().repo,
            id,
            token,
            operationId,
            input,
          ),
        );
      }
      if (path === "automation/cancel" && request.method === "POST") {
        const id = z
          .string()
          .uuid()
          .parse((await request.json()).id);
        await repo.authorize(principal, id, true);
        await repo.db.query(
          "UPDATE taskasaur.dispatches SET cancel_requested=true WHERE run_id=$1 AND workspace_id=$2",
          [id, workspaceId],
        );
        return Response.json({ ok: true });
      }
      if (path === "automation/signal" && request.method === "POST") {
        const input = z
          .object({
            id: z.string().uuid(),
            name: z.string().regex(/^[a-zA-Z0-9._-]{1,80}$/),
            data: z.unknown(),
            operationId: z.string().uuid(),
          })
          .parse(await request.json());
        return Response.json(
          await queueWorkflowSignal(
            repo,
            principal,
            input.id,
            input.name,
            (input.data ?? null) as Value,
            input.operationId,
          ),
        );
      }
      if (path === "automation/webhook" && request.method === "POST") {
        return Response.json(
          await rotateWorkflowHook(
            repo,
            principal,
            z
              .string()
              .uuid()
              .parse((await request.json()).id),
          ),
        );
      }
      if (path === "automation/run") {
        await repo.requirePlugin(principal, "automation-runtime");
        invariant(
          request.method === "POST",
          "METHOD_NOT_ALLOWED",
          "POST required",
        );
        const input = z
          .object({
            id: z.string().uuid(),
            targetDeviceId: z.string().uuid(),
            operationId: z.string().uuid().optional(),
            input: z.unknown().optional(),
          })
          .parse(await request.json());
        return Response.json(
          await dispatchWorkflow(
            repo,
            principal,
            input.id,
            input.targetDeviceId,
            { operationId: input.operationId, input: input.input as Value },
          ),
        );
      }
    },
    async tick({
      repo,
      workspaceId,
    }: {
      repo: Repository;
      workspaceId: string;
    }) {
      await processWorkflowTriggers(repo, workspaceId);
    },
    async beforeMutation({
      repo,
      principal,
      mutation,
      data,
      schema,
    }: import("@taskasaur/server-host").MutationHook) {
      const tx = repo.db;
      if (schema.id === "workflows" && data.published_version) {
        invariant(
          data.target_device_id,
          "VALIDATION_FAILED",
          "Published workflows require an explicit device",
        );
        const graph = validateGraph(data.graph);
        if (data.enabled && data.trigger_kind === "schedule")
          invariant(
            Number(data.schedule_seconds) >= 10 && data.schedule_start,
            "VALIDATION_FAILED",
            "An enabled schedule needs an interval and start instant",
          );
        if (data.enabled && data.trigger_kind === "event")
          invariant(
            typeof data.event_type === "string" &&
              /^taskasaur\.[a-zA-Z0-9._-]+\.v1$/.test(data.event_type),
            "VALIDATION_FAILED",
            "An event trigger needs a versioned committed event type",
          );
        for (const node of allNodes(graph))
          if (node.type === "typescript") {
            invariant(
              data.allow_trusted_code === true,
              "PERMISSION_DENIED",
              "Enable trusted TypeScript before publishing code nodes",
            );
            compileTypeScript(String(node.config.source));
          }
      }
    },
    async afterMutation({
      repo,
      principal,
      mutation,
      data,
      schema,
      record,
    }: import("@taskasaur/server-host").MutationHook) {
      const tx = repo.db;
      if (
        schema.id === "workflows" &&
        mutation.operation === "put" &&
        data.published_version
      )
        await tx.query(
          "INSERT INTO taskasaur.workflow_versions(workflow_id,version,graph,target_device_id,trusted_code) VALUES($1,$2,$3,$4,$5) ON CONFLICT(workflow_id,version) DO NOTHING",
          [
            mutation.resourceId,
            data.published_version,
            JSON.stringify(validateGraph(data.graph)),
            data.target_device_id,
            data.allow_trusted_code === true,
          ],
        );
    },
  };
}
export default {
  async activate(context: any) {
    const core = context.services.require("core.server");
    const backend = createBackend(core) as {
      activate?: (context: any) => Promise<unknown>;
    };
    return backend.activate?.(context);
  },
};

export { dispatchWorkflow } from "./dispatch";
export {
  processWorkflowTriggers,
  rotateWorkflowHook,
  receiveWorkflowHook,
  queueWorkflowSignal,
} from "./workflow-triggers";
