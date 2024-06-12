package com.poc.pubsub.service;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class PubsubService {

    @Value("${spring.cloud.gcp.project-id}")
    private String projectId;

    @Value("${spring.cloud.gcp.pubsub.emulator-host}")
    private String emulatorHost;

    private static final String TOPIC_CREATED = "Created topic: %s";
    private static final String MESSAGE_PUBLISHED = "Published message ID: %s";
    private static final String SUBSCRIPTION_CREATED = "Created a subscription with ordering: %s";

    private ManagedChannel channel;
    private TransportChannelProvider channelProvider;
    private CredentialsProvider credentialsProvider;

    public PubsubService() {
        //TODO: Alterar para o gcp cloud
        this.channel = ManagedChannelBuilder.forTarget("localhost:8085").usePlaintext().build();
        this.channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        this.credentialsProvider = NoCredentialsProvider.create();
    }

    public String createTopicExample(String topicId) throws IOException {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(TopicAdminSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build())) {

            TopicName topicName = TopicName.of(projectId, topicId);
            Topic topic = topicAdminClient.createTopic(topicName);
            System.out.println(TOPIC_CREATED.formatted(topic.getName()));
            return TOPIC_CREATED.formatted(topic.getName());
        }
    }

    public String publisherMessage(String topicId, String message) throws IOException, ExecutionException, InterruptedException {
        TopicName topicName = TopicName.of(projectId, topicId);
        Publisher publisher = null;
        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName)
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();

            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

            // Once published, returns a server-assigned message id (unique within the topic)
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            String messageId = messageIdFuture.get();
            System.out.println(MESSAGE_PUBLISHED.formatted(messageId));
            return MESSAGE_PUBLISHED.formatted(messageId).toString();
        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }

    public String createSubscriptionWithOrderingExample(String topicId, String subscriptionId) throws IOException {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build())) {

            ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
            ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

            Subscription subscription = subscriptionAdminClient.createSubscription(
                Subscription.newBuilder()
                    .setName(subscriptionName.toString())
                    .setTopic(topicName.toString())
                    // Set message ordering to true for ordered messages in the subscription.
                    .setEnableMessageOrdering(true)
                    .build());

            System.out.println(SUBSCRIPTION_CREATED.formatted(subscription.getAllFields()));
            return SUBSCRIPTION_CREATED.formatted(subscription.getAllFields());
        }
    }

    public void subscribeAsyncExample(String subscriptionId) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

        // Instantiate an asynchronous message receiver.
        MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
            // Handle incoming message, then ack the received message.
            System.out.println("Id: " + message.getMessageId());
            System.out.println("Data: " + message.getData().toStringUtf8());
            consumer.ack();
        };

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();
            // Start the subscriber.
            subscriber.startAsync().awaitRunning();
            System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
            // Allow the subscriber to run for 30s unless an unrecoverable error occurs.
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            // Shut down the subscriber after 30s. Stop receiving messages.
            subscriber.stopAsync();
        }
    }

    public void subscribeSyncExample(String subscriptionId, Integer numOfMessages) throws IOException {
        SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
            .setCredentialsProvider(credentialsProvider)
            .setTransportChannelProvider(channelProvider)
            .setTransportChannelProvider(SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                .build())
            .build();

        try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
            String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
            PullRequest pullRequest = PullRequest.newBuilder()
                .setMaxMessages(numOfMessages)
                .setSubscription(subscriptionName)
                .build();

            // Use pullCallable().futureCall to asynchronously perform this operation.
            PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

            // Stop the program if the pull response is empty to avoid acknowledging
            // an empty list of ack IDs.
            if (pullResponse.getReceivedMessagesList().isEmpty()) {
                System.out.println("No message was pulled. Exiting.");
                return;
            }

            List<String> ackIds = new ArrayList<>();
            for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
                // Handle received message
                // ...
                ackIds.add(message.getAckId());
            }

            // Acknowledge received messages.
            AcknowledgeRequest acknowledgeRequest =
                AcknowledgeRequest.newBuilder()
                    .setSubscription(subscriptionName)
                    .addAllAckIds(ackIds)
                    .build();

            // Use acknowledgeCallable().futureCall to asynchronously perform this operation.
            subscriber.acknowledgeCallable().call(acknowledgeRequest);
            System.out.println(pullResponse.getReceivedMessagesList());
        }
    }

}
