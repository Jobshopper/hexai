/* eslint-disable @typescript-eslint/no-unused-vars */
import { Message } from "@hexai/core";

import { AbstractLifecycle } from "@/helpers";
import { MessageChannel } from "@/channel";
import { InboundChannelAdapter } from "./inbound-channel-adapter";
import { IdempotencyViolationError } from "@/endpoint/idempotency-support";

export abstract class AbstractInboundChannelAdapter
    extends AbstractLifecycle
    implements InboundChannelAdapter
{
    protected outputChannel!: MessageChannel;

    public setOutputChannel(channel: MessageChannel): void {
        this.outputChannel = channel;
    }

    public override async start(): Promise<void> {
        if (!this.outputChannel) {
            throw new Error("output channel required");
        }

        await super.start();
    }

    protected async processMessage(): Promise<boolean> {
        if (!this.isRunning()) {
            return false;
        }

        const message = await this.receiveMessage();
        if (!message) {
            return false;
        }

        return await this.sendToOutputChannel(message);
    }

    protected async sendToOutputChannel(message: Message): Promise<boolean> {
        await this.beforeSend(message);

        let error: Error | undefined;
        try {
            await this.outputChannel.send(message);
        } catch (e) {
            error = e as Error;
            if (this.isIdempotencyViolated(error)) {
                return false;
            }
        }

        await this.afterSend(message, error);

        return !error;
    }

    protected isIdempotencyViolated(error: Error): boolean {
        return error instanceof IdempotencyViolationError;
    }

    protected async beforeSend(message: Message): Promise<void> {}

    protected async afterSend(message: Message, error?: Error): Promise<void> {}

    protected abstract receiveMessage(): Promise<Message | null>;
}
