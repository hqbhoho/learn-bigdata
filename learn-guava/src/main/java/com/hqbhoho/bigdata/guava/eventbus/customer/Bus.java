package com.hqbhoho.bigdata.guava.eventbus.customer;

public interface Bus {
    void register(Object subsciber);
    void unregister(Object subsciber);
    void post(Object event,String topic);
    void close();
    String getBusName();
}
