package com.hqbhoho.bigdata.guava.eventbus.monitor;

import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/09
 */
public class DirectoryChangeMonitor implements FileSystemMonitor {

    private final static Logger LOGGER = LoggerFactory.getLogger(DirectoryChangeMonitor.class);

    private Path monitorPath;
    private EventBus eventBus;
    private WatchService watchService;
    private volatile boolean start = false;

    public DirectoryChangeMonitor(Path monitorPath, EventBus eventBus) {
        this.monitorPath = monitorPath;
        this.eventBus = eventBus;
    }

    @Override
    public void startMonitor() throws Exception {
        // create an watch to a path
        LOGGER.info("[{}] begin to be Monitored...... ", monitorPath);
        this.watchService = FileSystems.getDefault().newWatchService();
        monitorPath.register(this.watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY);
        this.start = true;
        while (this.start) {
            WatchKey key = null;
            try {
                key = this.watchService.take();
                key.pollEvents().forEach(event -> {
                    WatchEvent.Kind<?> kind = event.kind();
                    Path context = (Path) event.context();
                    Path resolve = this.monitorPath.resolve(context);
                    this.eventBus.post(resolve + ": " + kind);
                });
            } catch (Exception e) {
                this.start=false;
            } finally {
                if (key != null) {
                    key.reset();
                }
            }
        }
    }

    @Override
    public void stopMonitor() throws Exception {
        LOGGER.info("{} monitor will be close......",this.monitorPath);
        if(null != this.watchService) {
            this.watchService.close();
        }
        this.start = false;
        LOGGER.info("{} monitor close done!",this.monitorPath);
    }
}
