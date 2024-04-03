import { beforeEach, describe, expect, it, test, vi } from "vitest";
import { Message } from "@hexai/core";
import { DummyMessage } from "@hexai/core/test";

import { AbstractInboundChannelAdapter } from "./abstract-inbound-channel-adapter";
import { MessageChannel } from "@/channel";
import { IdempotencyViolationError } from "@/endpoint/idempotency-support";

class InboundChannelAdapterForTest extends AbstractInboundChannelAdapter {
    private messages: Message[];

    constructor(messages: Message[] = []) {
        super();
        this.messages = [...messages];
    }

    protected async receiveMessage(): Promise<Message | null> {
        return this.messages.shift() ?? null;
    }

    public async poll(): Promise<boolean> {
        return await this.processMessage();
    }
}

describe("AbstractInboundChannelAdapter", () => {
    let adapter: InboundChannelAdapterForTest;
    let outputChannel: MessageChannel & {
        messagesSent: Message[];
    };
    const message = DummyMessage.create();

    beforeEach(() => {
        adapter = new InboundChannelAdapterForTest([message]);
        outputChannel = {
            messagesSent: [],
            async send(message: Message): Promise<void> {
                this.messagesSent.push(message);
            },
        };
        adapter.setOutputChannel(outputChannel);
    });

    it("does not send message if not running", async () => {
        await adapter.poll();

        expect(outputChannel.messagesSent).toEqual([]);
    });

    it("sends message", async () => {
        await adapter.start();

        await adapter.poll();

        expect(outputChannel.messagesSent).toEqual([message]);
    });

    test("beforeSend is called before sending message", async () => {
        vi.spyOn(adapter as any, "beforeSend").mockImplementation(() => {
            expect(outputChannel.messagesSent).toEqual([]);
        });
        await adapter.start();

        await adapter.poll();

        expect(outputChannel.messagesSent).toEqual([message]);
    });

    test("afterSend is called after sending message", async () => {
        vi.spyOn(adapter as any, "afterSend").mockImplementation(() => {
            expect(outputChannel.messagesSent).toEqual([message]);
        });
        await adapter.start();

        await adapter.poll();

        expect(outputChannel.messagesSent).toEqual([message]);
    });

    test("afterSend is called with error if sending message fails", async () => {
        const error = new Error("error");
        outputChannel.send = async () => {
            throw error;
        };
        const afterSend = vi
            .spyOn(adapter as any, "afterSend")
            .mockImplementation((...args: any[]) => {
                const [errorReceived] = args.slice(-1);
                expect(errorReceived).toEqual(error);
            });
        await adapter.start();

        await adapter.poll();

        expect(afterSend).toHaveBeenCalled();
    });

    test("afterSend is not called if the error is IdempotencyViolationError", async () => {
        const error = new IdempotencyViolationError();
        outputChannel.send = async () => {
            throw error;
        };
        const afterSend = vi.spyOn(adapter as any, "afterSend");
        await adapter.start();

        await adapter.poll();

        expect(afterSend).not.toHaveBeenCalled();
    });
})
