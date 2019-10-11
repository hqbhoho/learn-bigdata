package com.hqbhoho.bigdata.guava.eventbus.example;

import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/10
 */
public class TestEventListener2 extends TestEventListener {
    private final static Logger LOGGER = LoggerFactory.getLogger(TestEventListener.class);
    @Override
    @Subscribe
    public void test3(Integer event){
        LOGGER.info("TestEventListenter2 test3 get Integer event [{}]",event);
    }

}
