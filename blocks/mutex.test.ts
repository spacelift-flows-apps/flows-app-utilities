import { describe, it, expect, beforeEach, vi } from "vitest";

// --- Mock infrastructure (vi.hoisted runs before vi.mock) ---

const { mockKV, mockState } = vi.hoisted(() => {
  class MockKV {
    private store = new Map<string, { value: any; lockId?: string }>();

    async get(key: string) {
      const entry = this.store.get(key);
      return { key, value: entry?.value ?? null };
    }

    async set(pair: {
      key: string;
      value: any;
      ttl?: number;
      lock?: { id: string };
    }) {
      const existing = this.store.get(pair.key);

      if (pair.lock && existing?.lockId && existing.lockId !== pair.lock.id) {
        return false;
      }

      if (pair.ttl === 0) {
        this.store.delete(pair.key);
      } else {
        this.store.set(pair.key, {
          value: pair.value,
          lockId: pair.lock?.id,
        });
      }
      return true;
    }

    async setMany(pairs: any[]) {
      for (const pair of pairs) {
        if (!(await this.set(pair))) return false;
      }
      return true;
    }

    async list({ keyPrefix }: { keyPrefix: string }) {
      const pairs: { key: string; value: any }[] = [];
      for (const [key, entry] of this.store) {
        if (key.startsWith(keyPrefix)) {
          pairs.push({ key, value: entry.value });
        }
      }
      pairs.sort((a, b) => a.key.localeCompare(b.key));
      return { pairs };
    }

    clear() {
      this.store.clear();
    }

    getEntry(key: string) {
      return this.store.get(key) ?? null;
    }

    has(key: string) {
      return this.store.has(key);
    }
  }

  return {
    mockKV: new MockKV(),
    mockState: { pendingCounter: 0 },
  };
});

vi.mock("@slflows/sdk/v1", () => ({
  events: {
    createPending: vi.fn(
      async () => `pending-${++mockState.pendingCounter}`,
    ),
    emit: vi.fn(),
  },
  kv: {
    block: {
      get: vi.fn((key: string) => mockKV.get(key)),
      set: vi.fn((pair: any) => mockKV.set(pair)),
      setMany: vi.fn((pairs: any[]) => mockKV.setMany(pairs)),
      list: vi.fn((input: any) => mockKV.list(input)),
    },
  },
  lifecycle: {
    sync: vi.fn(),
  },
  timers: {
    set: vi.fn(async () => "timer-1"),
  },
}));

// --- Import block under test and mocked services ---

import mutex from "./mutex.ts";
import { events, lifecycle, timers } from "@slflows/sdk/v1";

// --- Helpers ---

function acquireEvent(id: string, config: { timeout?: number } = {}) {
  return { event: { id, inputConfig: config } } as any;
}

function releaseEvent(lockId: string) {
  return {
    event: { echo: { body: { lockId } }, inputConfig: {} },
  } as any;
}

function manualReleaseEvent() {
  return { event: { inputConfig: {} } } as any;
}

function timerEvent(payload: string) {
  return { timer: { id: "timer-1", payload } } as any;
}

async function acquireAndGrant(
  eventId: string,
  config: { timeout?: number } = {},
) {
  await mutex.inputs!.default.onEvent(acquireEvent(eventId, config));
  await mutex.onSync!({} as any);
  vi.clearAllMocks();
}

// --- Tests ---

describe("Mutex block", () => {
  beforeEach(() => {
    mockKV.clear();
    mockState.pendingCounter = 0;
    vi.clearAllMocks();
  });

  describe("Acquire input", () => {
    it("creates pending event and enqueues in KV", async () => {
      await mutex.inputs!.default.onEvent(
        acquireEvent("evt-1", { timeout: 60 }),
      );

      expect(events.createPending).toHaveBeenCalledWith({
        statusDescription: "Waiting for mutex",
      });

      const entry = mockKV.getEntry("evt:evt-1");
      expect(entry?.value).toEqual({ pendingId: "pending-1", timeout: 60 });

      expect(lifecycle.sync).toHaveBeenCalled();
    });
  });

  describe("onSync", () => {
    it("returns Available when queue is empty and no holder", async () => {
      const result = await mutex.onSync!({} as any);

      expect(result).toEqual({
        newStatus: "ready",
        customStatusDescription: "Available",
      });
    });

    it("returns Held when lock is already held", async () => {
      await mockKV.set({
        key: "currentHolder",
        value: "evt-1",
        lock: { id: "evt-1" },
      });

      const result = await mutex.onSync!({} as any);

      expect(result).toEqual({
        newStatus: "ready",
        customStatusDescription: "Held",
      });
      expect(events.emit).not.toHaveBeenCalled();
    });

    it("grants lock to oldest queued event", async () => {
      await mutex.inputs!.default.onEvent(
        acquireEvent("evt-1", { timeout: 60 }),
      );

      const result = await mutex.onSync!({} as any);

      expect(result).toEqual({
        newStatus: "ready",
        customStatusDescription: "Held",
      });
      expect(events.emit).toHaveBeenCalledWith(
        { lockId: "evt-1" },
        { complete: "pending-1", echo: true, parentEventId: "evt-1" },
      );
      expect(mockKV.has("evt:evt-1")).toBe(false);

      const holder = mockKV.getEntry("currentHolder");
      expect(holder?.value).toBe("evt-1");
      expect(holder?.lockId).toBe("evt-1");
    });

    it("sets timer when timeout is configured", async () => {
      await mutex.inputs!.default.onEvent(
        acquireEvent("evt-1", { timeout: 120 }),
      );
      await mutex.onSync!({} as any);

      expect(timers.set).toHaveBeenCalledWith(120, {
        inputPayload: "evt-1",
      });
    });

    it("does not set timer when timeout is omitted", async () => {
      await mutex.inputs!.default.onEvent(acquireEvent("evt-1"));
      await mutex.onSync!({} as any);

      expect(timers.set).not.toHaveBeenCalled();
    });

    it("grants lock to oldest event when multiple are queued", async () => {
      await mutex.inputs!.default.onEvent(
        acquireEvent("aaa", { timeout: 10 }),
      );
      await mutex.inputs!.default.onEvent(
        acquireEvent("bbb", { timeout: 20 }),
      );

      await mutex.onSync!({} as any);

      expect(events.emit).toHaveBeenCalledWith(
        { lockId: "aaa" },
        expect.objectContaining({ complete: "pending-1" }),
      );
      expect(mockKV.has("evt:bbb")).toBe(true);
      expect(mockKV.has("evt:aaa")).toBe(false);
    });
  });

  describe("Release input", () => {
    it("clears lock via echo and triggers sync", async () => {
      await acquireAndGrant("evt-1", { timeout: 60 });

      await mutex.inputs!.release.onEvent(releaseEvent("evt-1"));

      expect(mockKV.has("currentHolder")).toBe(false);
      expect(lifecycle.sync).toHaveBeenCalled();
    });

    it("releases current holder when event has no echo", async () => {
      await acquireAndGrant("evt-1");

      await mutex.inputs!.release.onEvent(manualReleaseEvent());

      expect(mockKV.has("currentHolder")).toBe(false);
      expect(lifecycle.sync).toHaveBeenCalled();
    });

    it("is a no-op when no echo and no current holder", async () => {
      await mutex.inputs!.release.onEvent(manualReleaseEvent());

      expect(lifecycle.sync).not.toHaveBeenCalled();
    });

    it("does not clear lock held by a different event (stale echo)", async () => {
      await acquireAndGrant("evt-a", { timeout: 60 });

      // evt-b queued, evt-a released, evt-b granted
      await mutex.inputs!.default.onEvent(
        acquireEvent("evt-b", { timeout: 60 }),
      );
      await mutex.inputs!.release.onEvent(releaseEvent("evt-a"));
      await mutex.onSync!({} as any);
      vi.clearAllMocks();

      // Stale echo for evt-a arrives
      await mutex.inputs!.release.onEvent(releaseEvent("evt-a"));

      expect(mockKV.has("currentHolder")).toBe(true);
      expect(mockKV.getEntry("currentHolder")?.value).toBe("evt-b");
      expect(lifecycle.sync).not.toHaveBeenCalled();
    });
  });

  describe("Timer auto-release", () => {
    it("clears lock and triggers sync", async () => {
      await acquireAndGrant("evt-1", { timeout: 60 });

      await mutex.onTimer!(timerEvent("evt-1"));

      expect(mockKV.has("currentHolder")).toBe(false);
      expect(lifecycle.sync).toHaveBeenCalled();
    });

    it("does not clear lock held by a different event", async () => {
      await acquireAndGrant("evt-a", { timeout: 60 });

      // evt-b queued, evt-a released, evt-b granted
      await mutex.inputs!.default.onEvent(
        acquireEvent("evt-b", { timeout: 60 }),
      );
      await mutex.inputs!.release.onEvent(releaseEvent("evt-a"));
      await mutex.onSync!({} as any);
      vi.clearAllMocks();

      // Stale timer for evt-a fires
      await mutex.onTimer!(timerEvent("evt-a"));

      expect(mockKV.has("currentHolder")).toBe(true);
      expect(mockKV.getEntry("currentHolder")?.value).toBe("evt-b");
      expect(lifecycle.sync).not.toHaveBeenCalled();
    });
  });

  describe("Full flow", () => {
    it("acquire, grant, release, next event granted", async () => {
      await mutex.inputs!.default.onEvent(
        acquireEvent("evt-1", { timeout: 60 }),
      );
      await mutex.inputs!.default.onEvent(
        acquireEvent("evt-2", { timeout: 60 }),
      );

      // First sync grants to evt-1
      let result = await mutex.onSync!({} as any);
      expect(result).toEqual({
        newStatus: "ready",
        customStatusDescription: "Held",
      });
      expect(events.emit).toHaveBeenCalledWith(
        { lockId: "evt-1" },
        expect.objectContaining({ complete: "pending-1" }),
      );

      // Release evt-1
      await mutex.inputs!.release.onEvent(releaseEvent("evt-1"));
      vi.clearAllMocks();

      // Second sync grants to evt-2
      result = await mutex.onSync!({} as any);
      expect(result).toEqual({
        newStatus: "ready",
        customStatusDescription: "Held",
      });
      expect(events.emit).toHaveBeenCalledWith(
        { lockId: "evt-2" },
        expect.objectContaining({ complete: "pending-2" }),
      );
    });
  });
});
