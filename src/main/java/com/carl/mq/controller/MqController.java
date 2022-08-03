package com.carl.mq.controller;

import com.carl.mq.constant.TopicEnum;
import com.carl.mq.producer.DefaultMessageProducer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;

/**
 * @author zjx
 * @date 2022/8/3 14:50
 */
@RestController
@RequestMapping("/mq")
public class MqController {

    private static final Logger logger = LoggerFactory.getLogger(MqController.class);

    private static final String DEFAULT_TAG = "default_tag";

    @Resource
    protected DefaultMessageProducer defaultMessageProducer;

    @GetMapping("/test1")
    public void test1(@RequestParam String msg) throws Exception {
        DefaultMQProducer producer = defaultMessageProducer.getProducer();
        Message message = new Message(TopicEnum.TEST_TOPIC_01.name(), DEFAULT_TAG, msg.getBytes(StandardCharsets.UTF_8));
        SendResult sendResult = producer.send(message);
        if (sendResult.getSendStatus().equals(SendStatus.SEND_OK)) {
            logger.info("send msg ok,content:{}", msg);
        }
    }

    @GetMapping("/test2")
    public void test2(@RequestParam String msg) throws Exception {
        DefaultMQProducer producer = defaultMessageProducer.getProducer();
        Message message = new Message(TopicEnum.TEST_TOPIC_02.name(), DEFAULT_TAG, msg.getBytes(StandardCharsets.UTF_8));
        SendResult sendResult = producer.send(message);
        if (sendResult.getSendStatus().equals(SendStatus.SEND_OK)) {
            logger.info("send msg ok,content:{}", msg);
        }
    }
}
