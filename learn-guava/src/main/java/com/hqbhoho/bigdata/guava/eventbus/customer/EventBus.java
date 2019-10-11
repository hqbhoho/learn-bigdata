package com.hqbhoho.bigdata.guava.eventbus.customer;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/10
 */
public class EventBus implements Bus {

    private final static String DEFAULT_BUS_NAME = "default-bus";
    private final static String DEFAULT_TOPIC_NAME = "default-topic";

    private String busName;
    private SubscribeRegistry registry;
    private Executor executor;
    private EventDispatcher eventDispatcher;


    private EventBus(Builder builder) {
        this.busName = builder.busName;
        this.registry = new SubscribeRegistry();
        if (builder.executor == null)
            this.executor = new SeqEventExecutor();
        else
            this.executor = builder.executor;
        this.eventDispatcher = new EventDispatcher(this.registry, this.executor,builder.eventExceptionHandler,this.busName);
    }

    public static EventBus.Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String busName = DEFAULT_BUS_NAME;
        private Executor executor;
        private EventExceptionHandler eventExceptionHandler;

        public Builder busName(String busName) {
            this.busName = busName;
            return this;
        }

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public Builder eventExceptionHandler(EventExceptionHandler eventExceptionHandler) {
            this.eventExceptionHandler = eventExceptionHandler;
            return this;
        }

        public EventBus build() {
            return new EventBus(this);
        }
    }

    /**
     * sequence process event
     */
    static class SeqEventExecutor implements Executor {
        @Override
        public void execute(Runnable runnable) {
            runnable.run();
        }
    }

    @Override
    public void register(Object subsciber) {
        this.registry.bind(subsciber);
    }

    @Override
    public void unregister(Object subsciber) {
        this.registry.unbind(subsciber);
    }

    public void post(Object event) {
        post(event, DEFAULT_TOPIC_NAME);
    }

    @Override
    public void post(Object event, String topic) {
        this.eventDispatcher.dispatch(event, topic);
    }

    @Override
    public void close() {
        if(this.executor instanceof ExecutorService){
            ((ExecutorService)this.executor).shutdown();
        }
    }

    @Override
    public String getBusName() {
        return this.busName;
    }
}
