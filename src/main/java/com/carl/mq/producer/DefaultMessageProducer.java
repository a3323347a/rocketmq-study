package com.carl.mq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author zjx
 * @date 2022/8/2 14:55
 */
@Component
@ConditionalOnProperty(name = "rocketmq.producer.group")
public class DefaultMessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageProducer.class);

    @Value("${rocketmq.producer.group}")
    private String producerGroup; //生产组名称

    @Value("${rocketmq.nameServer}")
    private String nameServer;

    private DefaultMQProducer producer = null;

    @PostConstruct
    public void initProducer() {
        producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(nameServer);
        producer.setVipChannelEnabled(false); //版本4.5.1之后默认为false,之前默认为true
        producer.setRetryAnotherBrokerWhenNotStoreOK(true); //发送失败时尝试发送另外一个broker,集群环境可以设置
        try {
            producer.setSendMsgTimeout(500);//超时时间
            producer.setRetryTimesWhenSendFailed(5);//失败最大重试次数,5次
            producer.start();
        } catch (Exception e) {
            logger.error("init rocketmq producer happen error", e);
            producer.shutdown();
        }
    }

    public DefaultMQProducer getProducer() {
        if (producer == null) {
            throw new RuntimeException("can not find rocketmq producer");
        }
        return producer;
    }

    @PreDestroy
    public void destroy() {
        if (producer != null) {
            producer.shutdown();
        }
    }
}
