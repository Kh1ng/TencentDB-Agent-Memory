import { describe, it, expect } from "vitest";
import { buildRecallContext } from "./server.js";
import type { RecallResult } from "../core/types.js";

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
