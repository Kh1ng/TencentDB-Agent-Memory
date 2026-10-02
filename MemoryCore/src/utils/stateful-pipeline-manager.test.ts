import { describe, it, expect } from "vitest";
import { LocalStateBackend } from "../core/state/local-backend.js";
import { StatefulPipelineManager, type PipelineConfig } from "./stateful-pipeline-manager.js";
import type { TaskPayload } from "../core/state/types.js";

const silentLogger = { info() {}, warn() {}, error() {}, debug() {} };

const PIPELINE_CONFIG: PipelineConfig = {
  everyNConversations: 1,
  enableWarmup: false,
  l1: { idleTimeoutSeconds: 9999 },
  l2: { delayAfterL1Seconds: 9999, minIntervalSeconds: 9999, maxIntervalSeconds: 9999, sessionActiveWindowHours: 999 },
};

function l1Task(id: string, sessionId: string): TaskPayload {
  return {
    id,
    type: "L1",
    instanceId: "default",
    sessionId,
    priority: 0,
    data: { instanceId: "default" },
    createdAt: 0,
  };
}

describe("StatefulPipelineManager.flushSession", () => {
  it("flushes persisted L0 after restart even when the in-memory count is zero", async () => {
    const backend = new LocalStateBackend();
    const manager = new StatefulPipelineManager(PIPELINE_CONFIG, backend, "default", silentLogger);
    await manager.start();
    let pending = "new project fact", durable = "", settled = false;
    const flush = manager.flushSession("restarted-session").then(() => { settled = true; });
    try {
      const task = await backend.consumeTask("test-worker", 50);
      expect(task?.type).toBe("L1");
      expect(task?.sessionId).toBe("restarted-session");
      expect(settled).toBe(false);
      // A worker's L1 runner consumes persisted L0, independent of buffer counts.
      durable = pending; pending = "";
      await backend.settleTask(task!);
      await flush;
      expect(durable).toBe("new project fact");
      expect(settled).toBe(true);
    } finally { await manager.destroy(); }
  });


  it("does not wait for tracked work from an unrelated session", async () => {
    const backend = new LocalStateBackend();
    const manager = new StatefulPipelineManager(PIPELINE_CONFIG, backend, "default", silentLogger);
    await manager.start();

    try {
      await backend.enqueueTask(l1Task("busy-task", "busy-session"));

      const busy = await backend.consumeTask("test-worker");
      const flush = manager.flushSession("idle-session", "default");
      const idle = await backend.consumeTask("test-worker",50);
      expect(idle?.sessionId).toBe("idle-session");
      await backend.settleTask(idle!);
      await flush;
      // The unrelated task remains tracked, but cannot block this barrier.
      await backend.settleTask(busy!);
    } finally {
      await manager.destroy();
    }
  });
});

describe("LocalStateBackend.waitForSessionSettle", () => {
  it("extends an active waiter across a retry replacement task", async () => {
    const backend = new LocalStateBackend();
    const original = l1Task("old-id", "retry-session");
    const replacement = l1Task("replacement-id", "retry-session");

    await backend.enqueueTask(original);

    let settled = false;
    const waiter = backend.waitForSessionSettle("default", "retry-session").then(() => {
      settled = true;
    });

    // PipelineWorker.reEnqueue performs these operations in this order:
    // replacement enqueue first, original task settlement in finally.
    await backend.enqueueTask(replacement);
    await backend.settleTask(original);
    await Promise.resolve();
    expect(settled).toBe(false);

    await backend.settleTask(replacement);
    await waiter;
    expect(settled).toBe(true);
  });
});
