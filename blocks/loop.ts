import {AppBlock, events, kv} from "@slflows/sdk/v1";
import {randomUUID} from "node:crypto";

export default {
    name: "For Loop",
    description:
        "A loop block that iterates over a range of values, emitting events for each iteration",
    category: "Control Flow",
    config: {
        range: {
            name: "Range",
            description: "The range of values to iterate over",
            type: {
                type: "object",
                properties: {
                    start: {
                        type: "number",
                        description: "Starting value (inclusive)",
                    },
                    end: {
                        type: "number",
                        description: "Ending value (exclusive)",
                    },
                    step: {
                        type: "number",
                        description: "Step increment",
                    },
                },
                required: ["start", "end"],
            },
            required: true,
            default: {start: 0, end: 10, step: 1},
        },
    },
    inputs: {
        default: {
            onEvent: async (input) => {
                const {range} = input.block.config;

                const start = range?.start ?? 0;
                const end = range?.end ?? 10;
                const step = range?.step ?? 1;

                if (step === 0) {
                    throw new Error("Step cannot be zero");
                }

                // Nothing to iterate if start is already past end
                const wouldIterate = step > 0 ? start < end : start > end;
                if (!wouldIterate) {
                    // Loop is complete, emit completion event
                    await events.emit(
                        {
                            iterations: 0,
                        },
                        {
                            outputKey: "default",
                        },
                    );
                    return;
                }

                // Initialize loop state in KV storage
                const loopId = randomUUID();
                const loopKey = `loop_${loopId}`;
                await kv.block.set({
                    key: loopKey,
                    value: {
                        start,
                        end,
                        step,
                        currentIndex: start,
                        iterationCount: 1,
                    },
                });

                console.info("Loop key:", loopKey);

                // Emit first iteration
                await events.emit(
                    {
                        index: start,
                        loopId: loopId,
                    },
                    {
                        outputKey: "loop",
                        echo: true,
                    },
                );
            },
        },
        loop: {
            config: {
                continueLoop: {
                    name: "Continue Loop",
                    description: "Whether to continue the loop iteration",
                    type: "boolean",
                    required: false,
                    default: true,
                },
            },
            onEvent: async (input) => {
                const {continueLoop} = input.event.inputConfig;

                console.info("Loop event:", input.event);
                const loopId = input.event.echo?.body.loopId;

                if (!loopId) {
                    return;
                }

                const loopKey = `loop_${loopId}`;

                const {value: loopState} = await kv.block.get(loopKey);

                const {start, end, step, currentIndex, iterationCount} =
                    loopState;
                const nextIndex = currentIndex + step;
                const nextIterationCount = iterationCount + 1;

                // Check if we should continue the loop (handles both positive and negative steps)
                const withinRange = step > 0 ? nextIndex < end : nextIndex > end;
                const shouldContinue = (continueLoop ?? true) && withinRange;

                if (shouldContinue) {
                    // Update state for next iteration
                    await kv.block.set({
                        key: loopKey,
                        value: {
                            start,
                            end,
                            step,
                            currentIndex: nextIndex,
                            iterationCount: nextIterationCount,
                        },
                    });

                    // Emit next iteration
                    await events.emit(
                        {
                            index: nextIndex,
                            loopId: loopId,
                        },
                        {
                            outputKey: "loop",
                            echo: true,
                        },
                    );
                } else {
                    // Clean up loop state before emitting completion
                    await kv.block.delete([loopKey]);

                    // Loop is complete, emit completion event
                    await events.emit(
                        {
                            iterations: iterationCount,
                        },
                        {
                            outputKey: "default",
                        },
                    );
                }
            },
        },
    },
    outputs: {
        default: {
            default: true,
            type: {
                type: "object",
                properties: {
                    iterations: {
                        type: "number",
                    },
                }

            },
        },
        loop: {
            type: {
                type: "object",
                properties: {
                    index: {
                        type: "number",
                    },
                }

            },
        },
    },
} satisfies AppBlock;
