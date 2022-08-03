package com.carl.mq.consumer;

import com.carl.mq.constant.TopicEnum;
import com.carl.mq.factory.MqExecutor;
import com.carl.mq.factory.MqExecutorFactory;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author zjx
 * @date 2022/8/2 15:19
 */
@Component
public abstract class AbstractConsumer implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(AbstractConsumer.class);

    private static final String DEFAULT_CONSUMER_TAG = "default_tag";

    @Value("${rocketmq.consumer.group}")
    private String consumerGroup;

    @Value("${rocketmq.nameServer}")
    private String nameServerAddr;

    @Resource
    private MqExecutorFactory mqExecutorFactory;

    /**
     * 获取主题
     */
    protected abstract TopicEnum getTopic();

    /**
     * 最大重试次数
     */
    protected int getMaxRetryTimes() { return 5; }

    protected int getConsumeThreadMin() {
        return 1;
    }

    protected int getConsumeThreadMax() {
        return 1;
    }

    protected String getConsumerTag() { return DEFAULT_CONSUMER_TAG; }


    /**
     * 容器启动时注册消费者
     *
     * @param args
     * @throws Exception
     */
    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("run rocketmq consumer");
        consume();
    }

    private void consume() throws MQClientException {
        String topic = getTopic().name();
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(nameServerAddr);
        consumer.setVipChannelEnabled(false);
        consumer.setConsumeThreadMin(getConsumeThreadMin());
        consumer.setConsumeThreadMax(getConsumeThreadMax());
        consumer.subscribe(topic, getConsumerTag());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
            for (MessageExt messageExt : list) {
                String msg;
                msg = new String(messageExt.getBody(), StandardCharsets.UTF_8);
                try {
                    boolean result = action(msg);
                    logger.info("消费第{}次,{}", messageExt.getReconsumeTimes(), msg);
                    if (!result) {
                        logger.error("执行方法异常，topic={}", topic);
                    }
                } catch (Exception e) {
                    int times = messageExt.getReconsumeTimes() + 1;
                    if (times >= getMaxRetryTimes()) {
                        logger.error("消费{}失超过{}次", topic, getMaxRetryTimes(), e);
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        logger.info("run rocketmq consumer,group:{} for topic:{}", consumerGroup, getTopic().name());
    }

    private boolean action(String msg) throws Exception {
        List<MqExecutor> executorList = mqExecutorFactory.getExecutors(getTopic());
        if (!executorList.isEmpty()) {
            for (MqExecutor executor : executorList) {
                executor.execute(msg);
            }
        }
        return true;
    }
}
