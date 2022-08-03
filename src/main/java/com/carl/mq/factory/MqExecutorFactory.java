package com.carl.mq.factory;

import com.carl.mq.annotation.DefinedMq;
import com.carl.mq.constant.TopicEnum;
import com.google.common.collect.ArrayListMultimap;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author zjx
 * @date 2022/8/2 16:14
 */
@Component
public class MqExecutorFactory implements ApplicationContextAware {

    private static ArrayListMultimap<TopicEnum, MqExecutor> executors = ArrayListMultimap.create();

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, MqExecutor> executorMap = applicationContext.getBeansOfType(MqExecutor.class);
        for (Map.Entry<String, MqExecutor> entry : executorMap.entrySet()) {
            MqExecutor mqExecutor = entry.getValue();
            DefinedMq definedMq = mqExecutor.getClass().getAnnotation(DefinedMq.class);
            if (definedMq != null) {
                executors.put(definedMq.topic(), mqExecutor);
            }
        }
    }

    public List<MqExecutor> getExecutors(TopicEnum topicEnum) {
        List<MqExecutor> mqExecutors = executors.get(topicEnum);
        return mqExecutors;
    }

}
