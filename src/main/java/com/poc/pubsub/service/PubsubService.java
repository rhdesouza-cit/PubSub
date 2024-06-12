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
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Service
public class PubsubService {

    @Value("${spring.cloud.gcp.project-id}")
    private String projectId;

    @Value("${spring.cloud.gcp.pubsub.emulator-host}")
    private String emulatorHost;

    public void createTopic(String topicId) throws Exception {
        createTopicV2(topicId);
        publisherExample(projectId, topicId);
/*        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
            topicAdminClient.createTopic(topicName);

            System.out.println("Topic created: " + topicName.getTopic());
        }*/
    }

    public void createSubscription(String subscriptionId, String topicId) throws Exception {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
            ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
            subscriptionAdminClient.createSubscription(subscriptionName, topicName, null, 10);
            System.out.println("Subscription created: " + subscriptionName.getSubscription());
        }
    }


    public void publishMessage(String topicId, String message) throws Exception {
        ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(topicName).build();
            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
            publisher.publish(pubsubMessage).get();
            System.out.println("Message published: " + message);
        } finally {
            if (publisher != null) {
                publisher.shutdown();
            }
        }
    }

    public void receiveMessages(String subscriptionId) throws Exception {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
        MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
            System.out.println("Message received: " + message.getData().toStringUtf8());
            consumer.ack();
        };
        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            subscriber.startAsync().awaitRunning();
            System.out.println("Listening for messages on " + subscriptionId);
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync();
            }
        }
    }


    public void createTopicV2(String topicId) throws IOException {
        String hostport = "localhost:8085";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
        try {
            TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

            // Set the channel and credentials provider when creating a `TopicAdminClient`.
            // Similarly for SubscriptionAdminClient
            TopicAdminClient topicClient = TopicAdminClient.create(
                TopicAdminSettings.newBuilder()
                    .setTransportChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build());

            TopicName topicName = TopicName.of(projectId, topicId);
            // Set the channel and credentials provider when creating a `Publisher`.
            // Similarly for Subscriber
            Publisher publisher =
                Publisher.newBuilder(topicName)
                    .setChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            channel.shutdown();
        }
    }

    public static void publisherExample(String projectId, String topicId)
        throws IOException, ExecutionException, InterruptedException {
        String hostport = "localhost:8085";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
        TopicName topicName = TopicName.of(projectId, topicId);
        TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

        Publisher publisher = null;
        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName)
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();

            String message = "Hello World!";
            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

            // Once published, returns a server-assigned message id (unique within the topic)
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            String messageId = messageIdFuture.get();
            System.out.println("Published message ID: " + messageId);
        } finally {
            channel.shutdown();
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }

}