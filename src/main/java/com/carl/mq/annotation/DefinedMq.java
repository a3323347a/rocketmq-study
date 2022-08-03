package com.carl.mq.annotation;

import com.carl.mq.constant.TopicEnum;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DefinedMq {
    TopicEnum topic();
}
