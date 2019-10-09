package com.hqbhoho.bigdata.guava.eventbus.monitor;

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
public class DirectoryChangeListener {

    private final static Logger LOGGER = LoggerFactory.getLogger(DirectoryChangeListener.class);

    @Subscribe
    public void onAction(String event){
        LOGGER.info("{}",event);
    }
}
