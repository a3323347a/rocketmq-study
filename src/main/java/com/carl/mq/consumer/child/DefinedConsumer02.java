package com.carl.mq.consumer.child;

import com.carl.mq.annotation.DefinedMq;
import com.carl.mq.constant.TopicEnum;
import com.carl.mq.consumer.AbstractConsumer;
import com.carl.mq.factory.MqExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author zjx
 * @date 2022/8/3 14:33
 */
@DefinedMq(topic = TopicEnum.TEST_TOPIC_02)
@Component
public class DefinedConsumer02 extends AbstractConsumer implements MqExecutor {

    private static final Logger logger = LoggerFactory.getLogger(DefinedConsumer02.class);

    @Override
    protected TopicEnum getTopic() {
        return TopicEnum.TEST_TOPIC_02;
    }

    @Override
    public boolean execute(String msg) throws Exception {
        if (msg == null) {
            logger.error("msg can not be null");
            return false;
        }
        logger.info("start to consume msg, current consumer:DefinedConsumer02, topic:TEST_TOPIC_02, start to consume msg, content:{}", msg);
        return true;
    }
}
