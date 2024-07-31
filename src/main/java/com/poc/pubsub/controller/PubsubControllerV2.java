package com.poc.pubsub.controller;

import com.poc.pubsub.service.PubsubServiceV2;
import jakarta.validation.constraints.NotNull;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping("/v2/pub-sub")
public class PubsubControllerV2 {

    private PubsubServiceV2 pubsubServiceV2;

    PubsubControllerV2(PubsubServiceV2 pubsubServiceV2) {
        this.pubsubServiceV2 = pubsubServiceV2;
    }

    @PostMapping("/create-topic")
    public String createTopic(@NotNull @RequestParam String topicId) throws IOException {
        return pubsubServiceV2.createTopic(topicId);
    }

    @PostMapping("/create-subscription")
    public String createSubscription(@RequestParam String subscriptionId, @RequestParam String topicId) throws Exception {
        return pubsubServiceV2.createSubscriptionWithOrderingExample(topicId, subscriptionId);
    }

    @PostMapping("/create-topic-dlq-subscription")
    public void createTopicDlq(
        @NotNull @RequestParam String topicId,
        @NotNull @RequestParam String topicDlqId,
        @NotNull @RequestParam String subscriptionId) throws IOException {

        pubsubServiceV2.createSubscriptionWithDeadLetterPolicy(subscriptionId, topicId, topicDlqId);
    }

    @PostMapping("/publish-message")
    public String publishMessage(@RequestBody String message, @RequestParam String topicId) throws Exception {
        return pubsubServiceV2.publisherMessage(topicId, message);
    }

    @GetMapping("/receive-message")
    public String receiveMessages(@RequestParam String subscriptionId, @RequestParam(defaultValue = "false") Boolean isDlq) {
        if (Boolean.TRUE.equals(isDlq)) {
            return pubsubServiceV2.subscribeAsyncExample(subscriptionId);
        }
        pubsubServiceV2.receiveMessagesWithDeliveryAttempts(subscriptionId);
        return null;
    }

}
