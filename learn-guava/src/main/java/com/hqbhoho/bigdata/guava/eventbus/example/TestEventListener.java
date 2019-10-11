package com.hqbhoho.bigdata.guava.eventbus.example;

import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/09
 */
public class TestEventListener {

    private final static Logger LOGGER = LoggerFactory.getLogger(TestEventListener.class);

    @Subscribe
    public void test1(String event){
        LOGGER.info("TestEventListenter test1 get String event [{}]",event);
    }

    /*@Subscribe
    public void test2(String event){
        throw new RuntimeException("Occur Exception......");
        // LOGGER.info("TestEventListenter test2 get String event [{}]",event);
    }*/

    @Subscribe
    public void test3(Integer event){
        LOGGER.info("TestEventListenter test3 get Integer event [{}]",event);
    }

    @Subscribe
    public void test4(String event){
        LOGGER.info("TestEventListenter test4 get String event [{}]",event);
    }

    @Subscribe
    public void test5(Integer event){
        LOGGER.info("TestEventListenter test5 get Integer event [{}]",event);
    }


}
