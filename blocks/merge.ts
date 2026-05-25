import { AppBlock, events, kv, timers } from "@slflows/sdk/v1";

const merge: AppBlock = {
  name: "Merge",
  category: "Transform",
  description:
    "Combines data from two separate event streams by matching events with the same key.\n\n" +
    "How it works:\n" +
    "- Primary events: Arrive first and wait for matching secondary events\n" +
    "- Secondary events: Bring payload data to merge with waiting primary events\n" +
    "- Matching: Events matched using identical keys from both streams\n" +
    "- Timeout: Primary events timeout if no secondary match arrives\n\n" +
    "Event flow:\n" +
    "1. Primary event arrives with matching key → waits for secondary\n" +
    "2. Secondary event with same key → merges immediately with waiting primary\n" +
    "3. If no match within timeout → primary emits with `matched: false`\n" +
    "4. If secondary has no waiting primary → emits to unmatched output\n\n" +
    "Common uses:\n" +
    "- Request/response correlation: Match API requests with their responses\n" +
    "- Multi-step workflows: Combine user actions with system responses\n" +
    "- Data enrichment: Add details from secondary source to primary events\n" +
    "- Approval workflows: Match requests with approval decisions\n" +
    "- Order processing: Combine orders with payment confirmations",
  config: {
    defaultMergeTimeout: {
      name: "Default merge timeout",
      description:
        "Default timeout for primary events waiting for secondary matches.\n\n" +
        "Purpose:\n" +
        "- Prevents primary events from waiting indefinitely\n" +
        "- Sets maximum time to wait for secondary event matches\n" +
        "- Can be overridden per primary event\n\n" +
        "Timeout behavior:\n" +
        "- Primary events timeout if no secondary match arrives in time\n" +
        "- Timeout triggers emission with `matched: false` and `payload: null`\n" +
        "- Prevents resource buildup from unmatched primary events",
      type: "number",
      default: 300, // 5 minutes
      required: false,
    },
  },
  inputs: {
    primary: {
      name: "Primary",
      description:
        "Primary event stream - events that arrive first and wait for secondary matches.\n\n" +
        "Role: These events initiate the merge process and wait for corresponding " +
        "secondary events with the same matching key.\n\n" +
        "Behavior:\n" +
        "- Stores matching key and waits for secondary event\n" +
        "- Times out if no secondary match arrives\n" +
        "- Emits with merged payload when secondary arrives\n" +
        "- Emits with `matched: false` on timeout",
      config: {
        matchingKey: {
          name: "Matching key",
          description:
            "**Identifier for matching** with secondary events that have the same key.\n\n**Key selection:**\n- Use unique identifiers that both event streams share\n- Ensure keys are consistent between primary and secondary events\n- Choose keys that won't accidentally match unrelated events\n\n**Examples:**\n- **Request correlation**: `outputs.request.requestId` or `outputs.request.correlationId`\n- **User workflows**: `outputs.request.userId + ':' + outputs.request.sessionId`\n- **Order processing**: `outputs.request.orderId`\n- **API calls**: `outputs.request.apiCallId` or `outputs.request.transactionId`\n- **File processing**: `outputs.request.fileId` or `outputs.request.batchId`\n\n**Important:** Both primary and secondary events must use identical keys to match.",
          type: "string",
          required: true,
        },
        mergeTimeout: {
          name: "Merge timeout",
          description:
            "**Event-specific timeout** in seconds (overrides the default timeout).\n\n**Use cases:**\n- **High priority events**: Shorter timeout for urgent processing\n- **Background tasks**: Longer timeout for non-critical operations\n- **User interactions**: Timeout based on expected user response time\n- **API calls**: Timeout matching external service SLA\n\n**Examples:**\n- Real-time alerts: 10-30 seconds\n- User approvals: 300-1800 seconds (5-30 minutes)\n- System processing: 600-3600 seconds (10-60 minutes)\n- Background jobs: 1800-7200 seconds (30 minutes - 2 hours)\n\n**Leave empty** to use the default timeout from block configuration.",
          type: "number",
          required: false,
        },
      },
      onEvent: async (input) => {
        try {
          const { matchingKey, mergeTimeout } = input.event.inputConfig;

          // Validate matching key
          if (!matchingKey) {
            console.error("Primary event missing matching key");
            return;
          }

          // Calculate effective timeout
          const effectiveTimeout =
            mergeTimeout || input.block.config.defaultMergeTimeout || 300; // 5 minutes default

          // Create a KV store entry for this primary event
          const kvKey = `merge:primary:${matchingKey}:${input.event.id}`;

          await kv.block.set({
            key: kvKey,
            value: { eventId: input.event.id },
          });

          // Set a timer to handle timeout case
          await timers.block.set(effectiveTimeout, {
            inputPayload: {
              kvKey,
              eventId: input.event.id,
            },
            description: `Timeout for merge key: ${matchingKey}`,
          });
        } catch (error) {
          console.error("Error processing primary event:", error);
        }
      },
    },
    secondary: {
      name: "Secondary",
      description:
        "**Secondary event stream** - events that provide payload data to merge with waiting primary events.\n\n**Role:** These events carry the data to be merged and trigger the merge when they match waiting primary events.\n\n**Behavior:**\n- Looks for primary events with matching key\n- If match found: merges payload with primary event\n- If no match found: emits to unmatched output\n- Immediately processes (doesn't wait)",
      config: {
        matchingKey: {
          name: "Matching key",
          description:
            "**Identifier for matching** with primary events - must be identical to the primary event's matching key.\n\n**Key consistency:**\n- Use exactly the same key format as primary events\n- Same data source and structure\n- Identical string values for successful matching\n\n**Examples (matching primary patterns):**\n- **Request correlation**: Same `requestId` from the original request\n- **User workflows**: Same `userId + ':' + sessionId` combination\n- **Order processing**: Same `orderId` from order creation\n- **API calls**: Same `apiCallId` or `transactionId`\n\n**Critical:** This key must exactly match a waiting primary event's key for the merge to succeed.",
          type: "string",
          required: true,
        },
        payload: {
          name: "Payload",
          description:
            "**Data to merge** with the primary event - this becomes the merged result.\n\n**What to include:**\n- Response data from API calls\n- Results from processing operations\n- User input or approval decisions\n- Enrichment data from external sources\n- Status updates or completion information\n\n**Examples:**\n- **API responses**: `outputs.data.responseBody` or complete API result\n- **User approvals**: `{approved: true, approvedBy: userId, timestamp: now}`\n- **Processing results**: `{status: 'completed', result: processedData}`\n- **External data**: Database query results or third-party service data\n- **File processing**: `{processedFile: fileUrl, metadata: fileInfo}`\n\n**This payload becomes the `payload` field in the merged output event.**",
          type: "any",
          required: true,
        },
      },
      onEvent: async (input) => {
        try {
          const { matchingKey, payload } = input.event.inputConfig;

          // Validate matching key
          if (!matchingKey) {
            console.error("Secondary event missing matching key");
            return;
          }

          // Search for matching primary events
          const prefix = `merge:primary:${matchingKey}:`;
          const { pairs } = await kv.block.list({ keyPrefix: prefix });

          if (pairs.length === 0) {
            // No matching primary events found, emit on unmatched output
            await events.emit(
              { matchingKey, payload },
              {
                outputKey: "unmatched",
                parentEventId: input.event.id,
              },
            );
            return;
          }

          // Collect all events to emit and keys to delete
          const promises: Promise<any>[] = [];
          const keysToDelete: string[] = [];

          for (const match of pairs) {
            const primaryEventId = match.value.eventId;

            // Add emit promise to the collection
            promises.push(
              events.emit(
                { matched: true, payload: payload },
                {
                  outputKey: "default",
                  parentEventId: primaryEventId,
                  secondaryParentEventIds: [input.event.id],
                },
              ),
            );

            // Add key to the collection for deletion
            keysToDelete.push(match.key);
          }

          if (keysToDelete.length > 0) {
            promises.push(kv.block.delete(keysToDelete));
          }

          // Execute all emit operations
          await Promise.all(promises);
        } catch (error) {
          console.error("Error processing secondary event:", error);
        }
      },
    },
  },
  outputs: {
    default: {
      default: true,
      name: "Merged",
      description:
        "**Successfully matched events** - emits when primary and secondary events are merged, or when primary events timeout.",
      possiblePrimaryParents: ["primary"],
      type: {
        type: "object",
        properties: {
          matched: {
            type: "boolean",
            description:
              "**Match status** - `true` if secondary event was found and merged, `false` if primary event timed out.\n\n**Usage:**\n- `true`: Successful merge, payload contains secondary data\n- `false`: Timeout occurred, payload is `null`\n- Use for conditional processing based on merge success",
          },
          payload: {
            type: "any",
            description:
              "**Merged data from secondary event** - contains the payload from the matching secondary event, or `null` if timeout.\n\n**Content when matched:**\n- Exact payload data from the secondary event\n- Can be any data type (object, string, number, array)\n- Use this data for further processing\n\n**Content when timeout:**\n- Always `null`\n- Indicates no secondary event arrived in time",
          },
        },
        required: ["matched"],
      },
    },
    unmatched: {
      name: "Unmatched",
      description:
        "**Secondary events with no waiting primary** - emits when a secondary event arrives but no primary event is waiting with the same key.",
      secondary: true,
      possiblePrimaryParents: ["secondary"],
      type: {
        type: "object",
        properties: {
          matchingKey: {
            type: "string",
            description:
              "**The key that had no match** - the matching key from the secondary event that couldn't find a waiting primary event.\n\n**Use for:**\n- Debugging missing primary events\n- Logging unmatched attempts\n- Creating fallback processing paths\n- Alerting on unexpected secondary events",
          },
          payload: {
            type: "any",
            description:
              "**Data from the unmatched secondary event** - the payload that couldn't be merged.\n\n**Handling unmatched events:**\n- Process independently without primary context\n- Log for debugging or monitoring\n- Queue for retry mechanisms\n- Alert on critical unmatched data\n- Use as fallback processing path",
          },
        },
        required: ["matchingKey", "payload"],
      },
    },
  },
  onTimer: async (input) => {
    try {
      const { kvKey, eventId } = input.timer.payload;

      // Skip if already matched
      if (!(await kv.block.get(kvKey)).value) return;

      await Promise.all([
        events.emit(
          { matched: false, payload: null },
          { parentEventId: eventId, outputKey: "default" },
        ),
        kv.block.delete([kvKey]),
      ]);
    } catch (error) {
      console.error("Error processing merge timeout:", error);
    }
  },
};

export default merge;
