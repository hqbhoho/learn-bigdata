package com.hqbhoho.bigdata.guava.eventbus.monitor;

import com.google.common.eventbus.EventBus;

import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/09
 */
public class DirectoryChangeExample {
    public static void main(String[] args) throws Exception {
        EventBus eventBus = new EventBus();
        eventBus.register(new DirectoryChangeListener());
        DirectoryChangeMonitor directoryChangeMonitor = new DirectoryChangeMonitor(Paths.get("E:\\test", ""), eventBus);
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        scheduledExecutorService.schedule(()->{
            try {
                directoryChangeMonitor.stopMonitor();
            } catch (Exception e) {
                e.printStackTrace();
            }
        },10, TimeUnit.SECONDS);
        scheduledExecutorService.shutdown();
        directoryChangeMonitor.startMonitor();

    }
}
