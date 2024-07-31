package com.poc.pubsub.controller;

import com.poc.pubsub.service.PubsubService;
import jakarta.validation.constraints.NotNull;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class PubsubController {

    private final PubsubService pubsubService;

    public PubsubController(PubsubService pubsubService) {
        this.pubsubService = pubsubService;
    }

    @PostMapping("/createTopic")
    public String createTopic(@RequestParam String topicId) throws Exception {
        return pubsubService.createTopicExample(topicId);
    }

    @PostMapping("/createSubscription")
    public String createSubscription(@RequestParam String subscriptionId, @RequestParam String topicId) throws Exception {
        return pubsubService.createSubscriptionWithOrderingExample(topicId, subscriptionId);
    }

    @PostMapping("/publishMessage")
    public String publishMessage(@RequestBody String message, @RequestParam String topicId) throws Exception {
        return pubsubService.publisherMessage(topicId, message);
    }

    @GetMapping("/receiveMessages")
    public String receiveMessages(@RequestParam String subscriptionId) {
        //return pubsubService.subscribeAsyncExample(subscriptionId);
        pubsubService.receiveMessagesWithDeliveryAttemptsExample(subscriptionId);
            return null;
    }

}
