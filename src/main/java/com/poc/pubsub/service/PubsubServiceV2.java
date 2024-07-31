package com.poc.pubsub.service;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
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
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.DeadLetterPolicy;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class PubsubServiceV2 {

    @Value("${spring.cloud.gcp.project-id}")
    private String projectId;

    @Value("${spring.cloud.gcp.pubsub.emulator-host}")
    private String emulatorHost;

    private static final String TOPIC_CREATED = "Created topic: %s";
    private static final String MESSAGE_PUBLISHED = "Published message ID: %s";
    private static final String SUBSCRIPTION_CREATED = "Created a subscription with ordering: %s";

    PubSubTemplate pubSubTemplate;

    private ManagedChannel channel;
    private TransportChannelProvider channelProvider;
    private CredentialsProvider credentialsProvider;

    public PubsubServiceV2() {
        this.channel = ManagedChannelBuilder.forTarget("localhost:8085").usePlaintext().build();
        this.channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        this.credentialsProvider = NoCredentialsProvider.create();
    }

    public String createTopic(String topicId) throws IOException {
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

    public void createSubscriptionWithDeadLetterPolicy(String subscriptionId, String topicId, String deadLetterTopicId)
        throws IOException {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build())) {

            ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
            ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of(projectId, subscriptionId);
            ProjectTopicName deadLetterTopicName = ProjectTopicName.of(projectId, deadLetterTopicId);

            DeadLetterPolicy deadLetterPolicy =
                DeadLetterPolicy.newBuilder()
                    .setDeadLetterTopic(deadLetterTopicName.toString())
                    // The maximum number of times that the service attempts to deliver a
                    // message before forwarding it to the dead letter topic. Must be [5-100].
                    .setMaxDeliveryAttempts(5)
                    .build();


            Subscription requestSubscription =
                Subscription.newBuilder()
                    .setName(subscriptionName.toString())
                    .setTopic(topicName.toString())
                    .setDeadLetterPolicy(deadLetterPolicy)
                    .setEnableExactlyOnceDelivery(true)
                    .build();

            Subscription subscription = subscriptionAdminClient.createSubscription(requestSubscription);

            System.out.println("Created subscription: " + subscription.getName());
            System.out.println(
                "It will forward dead letter messages to: "
                    + subscription.getDeadLetterPolicy().getDeadLetterTopic());
            System.out.println(
                "After "
                    + subscription.getDeadLetterPolicy().getMaxDeliveryAttempts()
                    + " delivery attempts.");
            // Remember to attach a subscription to the dead letter topic because
            // messages published to a topic with no subscriptions are lost.
        }
    }

    public String publisherMessage(String topicId, String message) throws IOException, ExecutionException, InterruptedException {
        TopicName topicName = TopicName.of(projectId, topicId);
        Publisher publisher = null;

        try {
            // Retry settings control how the publisher handles retry-able failures
            Duration initialRetryDelay = Duration.ofSeconds(5); // default: 100 ms
            double retryDelayMultiplier = 2.0; // back off for repeated failures, default: 1.3
            Duration maxRetryDelay = Duration.ofSeconds(200); // default : 60 seconds
            Duration initialRpcTimeout = Duration.ofSeconds(500); // default: 5 seconds
            double rpcTimeoutMultiplier = 2.0; // default: 1.0
            Duration maxRpcTimeout = Duration.ofSeconds(600); // default: 600 seconds
            Duration totalTimeout = Duration.ofSeconds(600); // default: 600 seconds

            RetrySettings retrySettings =
                RetrySettings.newBuilder()
                    .setInitialRetryDelay(initialRetryDelay)
                    .setRetryDelayMultiplier(retryDelayMultiplier)
                    .setMaxRetryDelay(maxRetryDelay)
                    .setInitialRpcTimeout(initialRpcTimeout)
                    .setRpcTimeoutMultiplier(rpcTimeoutMultiplier)
                    .setMaxRpcTimeout(maxRpcTimeout)
                    .setTotalTimeout(totalTimeout)
                    .build();

            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName)
                .setRetrySettings(retrySettings)
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

    public void receiveMessagesWithDeliveryAttempts(String subscriptionId) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

        // Instantiate an asynchronous message receiver.
        MessageReceiver receiver = new MessageReceiver() {
            @Override
            public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                try {
                    // Handle incoming message, then ack the received message.
                    System.out.println("Id: " + message.getMessageId());
                    System.out.println("Data: " + message.getData().toStringUtf8());
                    System.out.println("Delivery Attempt: " + Subscriber.getDeliveryAttempt(message));
                    consumer.ack();
                    Optional.of(null).orElseThrow(() -> new Exception("teste"));
                } catch (Exception e) {
                    System.out.println("Id: " + message.getMessageId() + " NACK Delivery Attempt: " + Subscriber.getDeliveryAttempt(message));
                    consumer.nack();
                }
            }
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
            subscriber.awaitTerminated(1, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            // Shut down the subscriber after 30s. Stop receiving messages.
            subscriber.stopAsync();
        }
    }

    public String subscribeAsyncExample(String subscriptionId) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
        final AtomicInteger qtdMessages = new AtomicInteger();
        // Instantiate an asynchronous message receiver.
        MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
            // Handle incoming message, then ack the received message.
            System.out.println("Id: " + message.getMessageId());
            System.out.println("Data: " + message.getData().toStringUtf8());
            consumer.ack();
            qtdMessages.getAndIncrement();
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
            subscriber.awaitTerminated(1, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            // Shut down the subscriber after 30s. Stop receiving messages.
            subscriber.stopAsync();
        }
        return String.format("Total de mensagems recebidas do tópico: %s", qtdMessages);
    }
}
