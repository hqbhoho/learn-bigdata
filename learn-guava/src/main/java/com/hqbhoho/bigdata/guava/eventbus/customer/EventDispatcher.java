package com.hqbhoho.bigdata.guava.eventbus.customer;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

/**
 * describe:
 * <p>
 * event process
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/10
 */
class EventDispatcher {
    private SubscribeRegistry registry;
    private Executor executor;
    private EventExceptionHandler eventExceptionHandler;
    private String busName;

    protected EventDispatcher(SubscribeRegistry registry, Executor executor,EventExceptionHandler eventExceptionHandler,String busName) {
        this.registry = registry;
        this.executor = executor;
        this.eventExceptionHandler = eventExceptionHandler;
        this.busName = busName;
    }

    public void dispatch(Object event, String topic) {
        ConcurrentLinkedQueue<Subscriber> topicSubecribers = this.registry.getTopicSubecribers(topic);
        if(topicSubecribers == null){
            EventContext eventContext = new EventContext(event,null,this.busName,topic);
            this.eventExceptionHandler.handle(new Exception("No one Subscribe this Topic " +topic),eventContext);
            return;
        }
        topicSubecribers
                .stream()
                .filter(subscriber -> subscriber.isActiveState() && subscriber.getSubscribeMethod().getParameterTypes()[0].isAssignableFrom(event.getClass())/*.isInstance(event)*/)
                .forEach(subscriber ->
                        this.executor.execute(
                                () -> {
                                    try {
                                        subscriber.getSubscribeMethod().invoke(subscriber.getSubscribeObject(), event);
                                    } catch (Exception e) {
                                        if(this.eventExceptionHandler != null){
                                            EventContext eventContext = new EventContext(event,subscriber,this.busName,topic);
                                            this.eventExceptionHandler.handle(e,eventContext);
                                        }else {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                        )
                );
    }
}
