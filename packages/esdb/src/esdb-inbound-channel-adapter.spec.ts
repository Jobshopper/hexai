import _ from "lodash";
import {
    beforeEach,
    describe,
    expect,
    it,
    SpyInstance,
    test,
    vi,
} from "vitest";
import { MessageChannel } from "@hexai/messaging";
import { Message } from "@hexai/core";
import {
    DummyMessage,
    expectMessagesToEqual,
    waitForMs,
} from "@hexai/core/test";
import {
    EventStoreDBClient,
    PersistentSubscriptionDoesNotExistError,
    persistentSubscriptionToStreamSettingsFromDefaults,
} from "@eventstore/db-client";

import { esdbClient, makeEsdbClient } from "@/test-fixtures";
import { EsdbHelper } from "@/esdb-helper";
import { EsdbInboundChannelAdapter } from "./esdb-inbound-channel-adapter";

const STREAM = "test-stream-inbound";
const GROUP = "test-group-inbound";

type MessageChannelForTest = MessageChannel & {
    messages: Message[];
    clear: () => void;
};

describe("ESDBInboundChannelAdapter", () => {
    let defaultAdapter: EsdbInboundChannelAdapter;
    let outputChannel: MessageChannelForTest;

    beforeEach(async () => {
        vi.resetAllMocks();
        vi.restoreAllMocks();
        await esdbClient.deleteStream(STREAM);
        await deleteConsumerGroup();

        outputChannel = createOutputChannel();

        defaultAdapter = makeAdapter();
        defaultAdapter.setOutputChannel(outputChannel);

        return async () => {
            outputChannel.clear();
            if (defaultAdapter.isRunning()) {
                await defaultAdapter.stop();
            }
        };
    });

    async function waitForPropagation() {
        await waitForMs(800);
    }

    function expectMessagesToBeDelivered(
        expected: Message[],
        actual: Message[]
    ) {
        expectMessagesToEqual(expected, actual);
    }

    async function expectMessagesToBeDistributedInRoundRobin(
        anotherOutputChannel: MessageChannelForTest
    ) {
        for (let i = 0; i < 3; i++) {
            const events = DummyMessage.createMany(6);
            await publishEvents(events);
            await waitForPropagation();
            expectMessagesToBeDelivered(
                events,
                _.zip(
                    outputChannel.messages,
                    anotherOutputChannel.messages
                ).flat() as Message[]
            );
            outputChannel.clear();
            anotherOutputChannel.clear();
        }
    }

    test("cannot start if no output channel is set", async () => {
        await expect(makeAdapter().start()).rejects.toThrow(
            "output channel required"
        );
    });

    it("creates consumer group upon start", async () => {
        await expect(getConsumerGroupInfo()).rejects.toThrowError(
            PersistentSubscriptionDoesNotExistError
        );

        await defaultAdapter.start();

        const info = await getConsumerGroupInfo();
        expect(info.status).toBe("Live");
    });

    test("default settings", async () => {
        await defaultAdapter.start();

        const info = await getConsumerGroupInfo();
        expect(info.settings).toContain({
            maxRetryCount: 10,
            messageTimeout: 30000,
            startFrom: "start",
        });
    });

    test("when consumer group is already created", async () => {
        await createConsumerGroup();

        // should not throw
        await defaultAdapter.start();
    });

    test("consuming", async () => {
        const events = DummyMessage.createMany(3);
        await publishEvents(events);

        await defaultAdapter.start();

        await waitForPropagation();
        expectMessagesToEqual(events, outputChannel.messages);
        outputChannel.messages = [];

        await publishEvents(DummyMessage.createMany(3));
        await waitForPropagation();

        expectMessagesToBeDelivered(events, outputChannel.messages);
    });

    it("acks when message is sent to output channel successfully", async () => {
        const spy = spySubscription();
        const event = DummyMessage.create();
        await publishEvents([event]);

        await defaultAdapter.start();

        await waitForPropagation();
        const acked = spy.ack.mock.calls[0][0];
        expect(acked.event.id).toBe(event.getMessageId());
        expect(spy.nack).not.toHaveBeenCalled();
    });

    it("nacks when message is not sent to output channel successfully", async () => {
        const spy = spySubscription();
        const event = DummyMessage.create();
        await publishEvents([event]);

        outputChannel.send = async () => {
            // do not send actual nack, because it will cause retries
            spy.nack.mockImplementation(async () => {
                return;
            });
            throw new Error("error");
        };

        await defaultAdapter.start();

        await waitForPropagation();
        const nacked = spy.nack.mock.calls[0][2];
        expect(nacked.event.id).toBe(event.getMessageId());
        expect(spy.ack).not.toHaveBeenCalled();
    });

    it("unsubscribes upon stop", async () => {
        await defaultAdapter.start();

        await defaultAdapter.stop();

        await waitForMs(100);
        const info = await getConsumerGroupInfo();
        expect(info.connections).toHaveLength(0);
    });

    test("at most once delivery__ROUND ROBIN", async () => {
        const anotherAdapter = makeAdapter(makeEsdbClient());
        const anotherOutputChannel = createOutputChannel();
        anotherAdapter.setOutputChannel(anotherOutputChannel);

        await Promise.all([defaultAdapter.start(), anotherAdapter.start()]);

        await expectMessagesToBeDistributedInRoundRobin(anotherOutputChannel);
    });
});

function makeAdapter(client: EventStoreDBClient = esdbClient) {
    const adapter = new EsdbInboundChannelAdapter({
        stream: STREAM,
        group: GROUP,
    });
    adapter.setApplicationContext({
        getEsdbClient: () => esdbClient,
    });

    return adapter;
}

function createOutputChannel(): MessageChannelForTest {
    return {
        messages: [],
        async send(message: Message): Promise<void> {
            this.messages.push(message);
        },
        clear() {
            this.messages = [];
        },
    };
}

async function createConsumerGroup(): Promise<void> {
    await esdbClient.createPersistentSubscriptionToStream(
        STREAM,
        GROUP,
        persistentSubscriptionToStreamSettingsFromDefaults()
    );
}

async function publishEvents(events: Message[]): Promise<void> {
    await new EsdbHelper(esdbClient).publishToStream(STREAM, events);
}

async function getConsumerGroupInfo() {
    return await esdbClient.getPersistentSubscriptionToStreamInfo(
        STREAM,
        GROUP
    );
}

async function deleteConsumerGroup() {
    try {
        await esdbClient.deletePersistentSubscriptionToStream(STREAM, GROUP);
    } catch (e) {
        if (e instanceof PersistentSubscriptionDoesNotExistError) {
            // ignore
        } else {
            throw e;
        }
    }
}

function spySubscription() {
    const ret: Record<string, SpyInstance> = {};

    const orig = esdbClient.subscribeToPersistentSubscriptionToStream;
    vi.spyOn(
        esdbClient,
        "subscribeToPersistentSubscriptionToStream"
    ).mockImplementation((stream: string, group: string) => {
        const subscription = orig.call(esdbClient, stream, group);

        ret["ack"] = vi.spyOn(subscription, "ack");
        ret["nack"] = vi.spyOn(subscription, "nack");

        return subscription;
    });

    return ret;
}
