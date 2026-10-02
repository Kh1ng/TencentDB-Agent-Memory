import http from "node:http";
import type { AddressInfo } from "node:net";
import { describe, it, expect } from "vitest";
import { buildRecallContext, TdaiGateway } from "./server.js";
import type { RecallResult } from "../core/types.js";
import { LocalStateBackend } from "../core/state/local-backend.js";
import { StatefulPipelineManager, type PipelineConfig } from "../utils/stateful-pipeline-manager.js";
import { PipelineWorker, type TaskExecutor } from "../services/pipeline-worker.js";
import type { TaskPayload } from "../core/state/types.js";

const silentLogger = { info() {}, warn() {}, error() {}, debug() {} };

const PIPELINE_CONFIG: PipelineConfig = {
  everyNConversations: 1,
  enableWarmup: false,
  l1: { idleTimeoutSeconds: 9999 },
  l2: { delayAfterL1Seconds: 9999, minIntervalSeconds: 9999, maxIntervalSeconds: 9999, sessionActiveWindowHours: 999 },
};

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

class WaitObservedLocalStateBackend extends LocalStateBackend {
  readonly waitEntered = deferred();

  override waitForSessionSettle(
    instanceId: string,
    sessionId: string,
    teamId?: string,
    agentId?: string,
  ): Promise<void> {
    const settling = super.waitForSessionSettle(instanceId, sessionId, teamId, agentId);
    // super has synchronously installed the waiter before returning.
    this.waitEntered.resolve();
    return settling;
  }
}

type GatewayHttpTestSeam = {
  core: {
    handleSessionEnd(sessionKey: string): Promise<void>;
    handleBeforeRecall(query: string, sessionKey: string): Promise<RecallResult>;
  };
  logger: typeof silentLogger;
  handleRequest(req: http.IncomingMessage, res: http.ServerResponse): Promise<void>;
};

async function listen(server: http.Server): Promise<string> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address() as AddressInfo;
  return `http://127.0.0.1:${address.port}`;
}

async function close(server: http.Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });
}

it("attributes rejected GAH captures without recording credentials or request content", async () => {
  const logs: string[] = [];
  const gateway = new TdaiGateway({ server: { host: "127.0.0.1", port: 0, apiKey: "test-secret", corsOrigins: [] } });
  const seam = gateway as unknown as GatewayHttpTestSeam;
  seam.logger = { ...silentLogger, info: (message: string) => { logs.push(message); } };
  const server = http.createServer((req, res) => { void seam.handleRequest(req, res); });
  const base = await listen(server);
  try {
    const response = await fetch(`${base}/capture`, {
      method: "POST", headers: { "X-GAH-Caller": "cli", Authorization: "Bearer wrong-secret" }, body: "private-content",
    });
    expect(response.status).toBe(401);
    await response.text();
    expect(logs.join("\n")).toContain("status=401 caller=cli source=");
    expect(logs.join("\n")).not.toMatch(/wrong-secret|test-secret|private-content/);
  } finally { await close(server); }
});

// Regression test for a bug where POST /recall silently dropped
// prependContext (the actual session-specific L1 memories) and returned
// only appendSystemContext (a generic, session-independent persona/scene
// blob) -- meaning callers never saw query-relevant recalled memory.
describe("buildRecallContext", () => {
  it("includes both the stable persona/scene block and the per-session recalled memories", () => {
    const result: RecallResult = {
      appendSystemContext: "<user-persona>...</user-persona>",
      prependContext: "<relevant-memories>project X shipped Y</relevant-memories>",
    };
    const context = buildRecallContext(result);
    expect(context).toContain("<user-persona>");
    expect(context).toContain("project X shipped Y");
  });

  it("omits missing parts instead of inserting empty sections", () => {
    expect(buildRecallContext({ prependContext: "facts only" })).toBe("facts only");
    expect(buildRecallContext({ appendSystemContext: "persona only" })).toBe("persona only");
    expect(buildRecallContext({})).toBe("");
  });
});

describe("TdaiGateway /session/end barrier (#958)", () => {
  it("holds the HTTP response until extraction commits and immediate HTTP recall sees it", async () => {
    const backend = new WaitObservedLocalStateBackend();
    const manager = new StatefulPipelineManager(PIPELINE_CONFIG, backend, "default", silentLogger);
    await manager.start();

    const extractionStarted = deferred();
    const extractionGate = deferred();
    const visibleMemory = new Map<string, string>();
    const executor: TaskExecutor = {
      async executeL1(task: TaskPayload) {
        extractionStarted.resolve();
        await extractionGate.promise;
        visibleMemory.set(task.sessionId, "fresh-value");
      },
      async executeL2() {},
      async executeL3() {},
    };
    const worker = new PipelineWorker(backend, executor, { concurrency: 1 }, silentLogger);

    const gateway = new TdaiGateway({
      server: { host: "127.0.0.1", port: 0, apiKey: undefined, corsOrigins: [] },
    });
    const gatewaySeam = gateway as unknown as GatewayHttpTestSeam;
    gatewaySeam.logger = silentLogger;
    gatewaySeam.core = {
      handleSessionEnd: (sessionKey) => manager.flushSession(sessionKey, "default"),
      async handleBeforeRecall(_query, sessionKey) {
        const memory = visibleMemory.get(sessionKey) ?? "stale-value";
        return {
          prependContext: memory,
          recalledL1Memories: [{ content: memory, score: 1, type: "test" }],
          recallStrategy: "test",
        };
      },
    };

    // The server is transport-only test scaffolding. All URL dispatch, body
    // parsing, endpoint handlers, and response serialization run through the
    // production TdaiGateway.handleRequest implementation.
    const server = http.createServer((req, res) => {
      void gatewaySeam.handleRequest(req, res);
    });
    const origin = await listen(server);
    let sessionEndRequest: Promise<Response> | undefined;

    try {
      const sessionKey = "sess-958";
      visibleMemory.set(sessionKey, "stale-value");
      await manager.notifyConversation(sessionKey, [], "default");
      await worker.start();
      await extractionStarted.promise;

      let sessionEndSettled = false;
      sessionEndRequest = fetch(`${origin}/session/end`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ session_key: sessionKey }),
      });
      const observedSessionEnd = sessionEndRequest.then((response) => {
        sessionEndSettled = true;
        return response;
      });

      // The observer proves the HTTP handler reached the production barrier.
      // A microtask checkpoint then proves no response was emitted while the
      // extraction latch remains held, without clocks, polling, or a timeout.
      await backend.waitEntered.promise;
      await Promise.resolve();
      expect(sessionEndSettled).toBe(false);

      extractionGate.resolve();
      const sessionEndResponse = await observedSessionEnd;
      expect(sessionEndResponse.status).toBe(200);
      expect(await sessionEndResponse.json()).toEqual({ flushed: true });

      const recallResponse = await fetch(`${origin}/recall`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: "current value", session_key: sessionKey }),
      });
      expect(recallResponse.status).toBe(200);
      expect(await recallResponse.json()).toMatchObject({
        context: "fresh-value",
        memory_count: 1,
        code: 0,
      });
    } finally {
      extractionGate.resolve();
      await sessionEndRequest?.catch(() => undefined);
      await close(server);
      await worker.stop();
      await manager.destroy();
    }
  });
});
