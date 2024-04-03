import { Message } from "@hexai/core";

export interface IdempotencySupport {
    isDuplicate(key: string, message: Message): Promise<boolean>;
    markAsProcessed(key: string, message: Message): Promise<void>;
}

export class IdempotencyViolationError extends Error {
    constructor() {
        super("Idempotency violation");
        this.name = "IdempotencyViolationError";
    }
}