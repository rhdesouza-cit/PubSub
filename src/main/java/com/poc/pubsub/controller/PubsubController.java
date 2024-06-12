package com.poc.pubsub.controller;

import com.poc.pubsub.service.PubsubService;
import com.poc.pubsub.service.PubsubServiceV2;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class PubsubController {

    private final PubsubService pubSubService;
    private final PubsubServiceV2 pubsubServiceV2;

    public PubsubController(PubsubService pubSubService,
                           PubsubServiceV2 pubsubServiceV2){
        this.pubSubService = pubSubService;
        this.pubsubServiceV2 = pubsubServiceV2;
    }

    @PostMapping("/createTopic")
    public String createTopic(@RequestParam String topicId) throws Exception {
//        pubsubServiceV2.createTopicV2(topicId);
        return pubsubServiceV2.createTopicExample(topicId);
        //pubSubService.createTopic(topicId);
    }

    @PostMapping("/createSubscription")
    public String createSubscription(@RequestParam String subscriptionId, @RequestParam String topicId) throws Exception {
        return pubsubServiceV2.createSubscriptionWithOrderingExample(topicId, subscriptionId);
    }

    @PostMapping("/publishMessage")
    public String publishMessage(@RequestParam String topicId, @RequestParam String message) throws Exception {
        return pubsubServiceV2.publisherMessage(topicId, message);

        //pubSubService.publishMessage(topicId, message);
    }

    @GetMapping("/receiveMessages")
    public void receiveMessages(@RequestParam String subscriptionId) throws Exception {
        pubsubServiceV2.subscribeAsyncExample(subscriptionId);
        //pubsubServiceV2.subscribeSyncExample(subscriptionId, 10);
        //pubSubService.receiveMessages(subscriptionId);
    }

}
