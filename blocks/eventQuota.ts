import { AppBlock, events, JsonSchema, kv } from "@slflows/sdk/v1";

interface LimitRejectedEvent {
  eventId: string;
  body: Record<string, unknown>;
}

interface FlowEventUsage {
  flowId: string;
  flowName: string;
  used: number;
  limit: number;
  periodEnd: string;
  // Set only for a flow at its limit: the first event the limit stopped in the
  // current episode (since the flow last processed anything), if still known.
  limitRejectedEvent?: LimitRejectedEvent;
}

// Calls the executor backend directly, the same way the app runtime does, so
// we don't depend on the SDK/runtime exposing a `quota` helper. The backend
// scopes the result to the project the app is installed in.
const getFlowEventUsages = async (
  flowIds: string[] | undefined,
): Promise<FlowEventUsage[]> => {
  const response = await fetch(
    `${process.env["FLOWS_ENDPOINT"]}/executorbackend/get_flow_event_usage`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env["FLOWS_APP_TOKEN"]}`,
        "Content-Type": "application/json",
      },
      // The backend treats a present-but-empty `flowIds` as "match nothing",
      // so omit it entirely to mean "all flows in the project".
      body: JSON.stringify(flowIds?.length ? { flowIds } : {}),
    },
  );

  if (!response.ok) {
    throw new Error(
      `Failed to get flow event usage: ${response.status} ${await response.text()}`,
    );
  }

  const { data } = await response.json();
  return data.flows;
};

const usageProperties: Record<string, JsonSchema> = {
  flowId: {
    type: "string",
    description: "ID of the flow.",
  },
  flowName: {
    type: "string",
    description: "Name of the flow.",
  },
  used: {
    type: "number",
    description: "Events counted so far this month.",
  },
  limit: {
    type: "number",
    description: "Monthly event limit for the flow.",
  },
  remaining: {
    type: "number",
    description: "Events left before the limit is reached.",
  },
  percentUsed: {
    type: "number",
    description: "Share of the limit used so far, as a percentage.",
  },
  periodEnd: {
    type: "string",
    description: "When the current monthly period ends (ISO 8601).",
  },
};

const usageRequired = [
  "flowId",
  "flowName",
  "used",
  "limit",
  "remaining",
  "percentUsed",
  "periodEnd",
];

// `body` is redacted by the backend the same way as event bodies in the UI.
const rejectedEventSchema: JsonSchema = {
  type: "object",
  properties: {
    eventId: { type: "string" },
    body: { type: "object", additionalProperties: true },
  },
  required: ["eventId", "body"],
};

const THRESHOLD_ALERTED_PREFIX = "alerted:threshold:";
const LIMIT_ALERTED_PREFIX = "alerted:limit:";

type Usage = Omit<FlowEventUsage, "limitRejectedEvent"> & {
  remaining: number;
  percentUsed: number;
};

const toUsage = ({
  flowId,
  flowName,
  used,
  limit,
  periodEnd,
}: FlowEventUsage): Usage => ({
  flowId,
  flowName,
  used,
  limit,
  remaining: Math.max(0, limit - used),
  percentUsed: limit > 0 ? (used / limit) * 100 : 100,
  periodEnd,
});

// Emits on `outputKey` unless the flow already alerted for the same `marker`.
const emitOnce = async (
  keyPrefix: string,
  flowId: string,
  marker: string,
  body: Record<string, unknown>,
  outputKey: string,
) => {
  const key = `${keyPrefix}${flowId}`;
  const { value: alerted } = await kv.block.get(key);
  if (alerted === marker) {
    return;
  }

  await Promise.all([
    events.emit(body, { outputKey }),
    kv.block.set({ key, value: marker }),
  ]);
};

const eventQuota: AppBlock = {
  name: "Event Quota",
  category: "Control",
  description:
    "Polls the monthly event usage of flows in this project and fires " +
    "events when a flow approaches or reaches its monthly limit.\n\n" +
    "How it works:\n" +
    "- On a schedule (every 5 minutes by default), the block reads usage " +
    "and limits for the configured flows, or all flows in the project if " +
    "none are configured\n" +
    "- Polling itself emits no events, so it doesn't eat into the quota\n" +
    "- The first time a flow's usage reaches `threshold` percent of its " +
    "limit in a monthly period, the `default` output fires for that flow\n" +
    "- When a flow at its limit starts dropping events, the `limitReached` " +
    "output fires with the first event the limit stopped\n" +
    "- Each output fires once per flow, and re-arms when the next period " +
    "starts or the flow's limit is changed\n\n" +
    "Notes:\n" +
    "- The usage counter is maintained asynchronously and trails live " +
    "traffic by up to ~25 minutes, so treat the numbers as recent rather " +
    "than exact\n" +
    "- Once a flow reaches its limit, its own blocks stop running. Place " +
    "this block in a separate monitoring flow so its alerts still get " +
    "delivered\n\n" +
    "Use cases:\n" +
    "- Budget alerts: notify when a flow approaches its monthly limit\n" +
    "- Incident follow-up: see which event was dropped when a flow hit its " +
    "limit",

  autoconfirm: true,
  config: {
    flowIds: {
      name: "Flows",
      description:
        "Flows to monitor. Leave empty to monitor every flow in the project. " +
        "Flows outside the project are ignored.",
      type: ["string"],
      required: false,
      suggestValues: async ({ searchPhrase }) => {
        const search = searchPhrase?.toLowerCase() ?? "";
        const flows = await getFlowEventUsages(undefined);
        return {
          suggestedValues: flows
            .filter((flow) => flow.flowName.toLowerCase().includes(search))
            .map((flow) => ({ label: flow.flowName, value: flow.flowId })),
        };
      },
    },
    threshold: {
      name: "Threshold (%)",
      description:
        "Percentage of a flow's monthly event limit at or above which the " +
        "`default` output fires.\n\n" +
        "Examples:\n" +
        "- `80` → fire once 80% of the quota is used\n" +
        "- `100` → fire only once the quota is exhausted",
      type: "number",
      required: true,
      default: 80,
    },
  },

  schedules: {
    poll: {
      description: "How often to check the flows' event usage.",
      customizable: true,
      definition: {
        type: "frequency",
        frequency: { interval: 5, unit: "seconds" },
      },
      onTrigger: async ({ block: { config } }) => {
        const threshold = config.threshold as number;
        if (!(threshold >= 0)) {
          throw new Error(
            `Event Quota requires threshold >= 0 (got threshold=${threshold})`,
          );
        }

        const flows = await getFlowEventUsages(
          config.flowIds as string[] | undefined,
        );
        const alerts: Promise<void>[] = [];
        for (const flow of flows) {
          const usage = toUsage(flow);
          // Keyed on period and limit, so a limit changed in the UI mid-period
          // re-arms the alert.
          if (usage.percentUsed >= threshold) {
            alerts.push(
              emitOnce(
                THRESHOLD_ALERTED_PREFIX,
                usage.flowId,
                `${usage.periodEnd}:${usage.limit}`,
                usage,
                "default",
              ),
            );
          }
          // Keyed on the episode's first rejected event: it stays the same while
          // the flow is blocked and changes once the flow recovers (limit
          // raised, new period) and gets blocked again.
          const rejected = flow.limitRejectedEvent;
          if (rejected) {
            alerts.push(
              emitOnce(
                LIMIT_ALERTED_PREFIX,
                usage.flowId,
                rejected.eventId,
                { ...usage, limitRejectedEvent: rejected },
                "limitReached",
              ),
            );
          }
        }

        await Promise.all(alerts);
      },
    },
  },

  outputs: {
    default: {
      default: true,
      name: "Threshold Reached",
      description:
        "Triggered once per flow per monthly period (and again if the limit " +
        "is changed), the first time the " +
        "flow's usage reaches the configured threshold. Body contains the " +
        "flow's current usage.",
      type: {
        type: "object",
        properties: usageProperties,
        required: usageRequired,
      },
    },
    limitReached: {
      name: "Limit Reached",
      description:
        "Triggered once each time a flow at its monthly limit starts " +
        "dropping events. It fires again only after the flow recovers (the " +
        "limit is raised or a new period starts) and is blocked again. Body " +
        "contains the flow's current usage and `limitRejectedEvent`, the " +
        "first event the limit stopped since the flow was blocked.",
      secondary: true,
      type: {
        type: "object",
        properties: {
          ...usageProperties,
          limitRejectedEvent: {
            ...rejectedEventSchema,
            description:
              "The first event the limit stopped since the flow was blocked.",
          },
        },
        required: [...usageRequired, "limitRejectedEvent"],
      },
    },
  },
};

export default eventQuota;
