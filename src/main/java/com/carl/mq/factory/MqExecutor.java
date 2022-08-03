package com.carl.mq.factory;

/**
 * @author zjx
 * @date 2022/8/2 16:14
 */
public interface MqExecutor {
    boolean execute(String msg) throws Exception;
}
