package com.hqbhoho.bigdata.guava.eventbus.customer;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/10
 */
public class EventContext {
    private Object event;
    private Subscriber subscriber;
    private String busName;
    private String topic;

    public EventContext(Object event, Subscriber subscriber, String busName, String topic) {
        this.event = event;
        this.subscriber = subscriber;
        this.busName = busName;
        this.topic = topic;
    }

    public Object getEvent() {
        return event;
    }

    public Subscriber getSubscriber() {
        return subscriber;
    }

    public String getBusName() {
        return busName;
    }

    public String getTopic() {
        return topic;
    }
}
