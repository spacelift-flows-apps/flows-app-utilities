import {AppBlock, events, kv} from "@slflows/sdk/v1";
import {randomUUID} from "node:crypto";

export default {
    name: "For Loop",
    description:
        "A loop block that iterates over a range of values, emitting events for each iteration",
    category: "Testing",
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

                // Initialize loop state in KV storage
                const loopId= randomUUID();
                const loopKey = `loop_${loopId}`;
                await kv.block.set({
                    key: loopKey,
                    value: {
                        start,
                        end,
                        step,
                        currentIndex: start,
                        iterationCount: 0,
                    },
                });

                console.info("Loop key:",loopKey)

                // Emit first iteration
                await events.emit(
                    {
                        index: start,
                        loopId: loopId,
                    },
                    {
                        outputKey: "loop",
                        echo: true,
                    }
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

                console.info("Loop event:",input.event)
                const loopId = input.event.echo?.body.loopId;

                if (!loopId) {
                    return;
                }

                const loopKey = `loop_${loopId}`;

                const {value: loopState} = await kv.block.get(loopKey);

                const {start, end, step, value, currentIndex, iterationCount} = loopState;
                const nextIndex = currentIndex + step;
                const nextIterationCount = iterationCount + 1;

                // Check if we should continue the loop
                const shouldContinue = (continueLoop ?? true) && nextIndex < end;

                if (shouldContinue) {
                    // Update state for next iteration
                    await kv.block.set({
                        key: loopKey,
                        value: {
                            start,
                            end,
                            step,
                            value,
                            currentIndex: nextIndex,
                            iterationCount: nextIterationCount,
                        },
                    });

                    // Emit next iteration
                    await events.emit(
                        {
                            index: nextIndex,
                            loopId: loopId,
                            value,
                        },
                        {
                            outputKey: "loop",
                            echo: true,
                        }
                    );
                } else {
                    // Loop is complete, emit completion event
                    await events.emit(
                        {
                            value,
                            iterations: nextIterationCount,
                        },
                        {
                            outputKey: "default",
                        }
                    );

                    // Clean up loop state
                    await kv.block.delete([loopKey]);
                }
            },
        },
    },
    outputs: {
        default: {
            default: true,
            config: {
                value: {
                    name: "Value",
                    type: "any",
                },
                iterations: {
                    name: "Iterations",
                    type: "number",
                },
            },
        },
        loop: {
            config: {
                index: {
                    name: "Index",
                    type: "number",
                },
                value: {
                    name: "Value",
                    type: "any",
                },
            },
        },
    },
} satisfies AppBlock;