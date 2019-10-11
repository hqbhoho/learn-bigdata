package com.hqbhoho.bigdata.guava.eventbus.customer;

import com.google.common.base.Preconditions;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * describe:
 * <p>
 * store subscriber information
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/10
 */
class SubscribeRegistry {

    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Subscriber>> subscriberContainer
            = new ConcurrentHashMap<>();

    public void bind(Object subscriber) {
        Preconditions.checkNotNull(subscriber, "subscriber can't be null!");
        Class<?> subscribeClass = subscriber.getClass();
        Method[] methods = subscribeClass.getMethods();
        Arrays.asList(methods)
                .stream()
                .filter(method ->
                        method.isAnnotationPresent(Subscribe.class)
                                && method.getParameterCount() == 1
                                && "void".equals(method.getReturnType().getName()))
                .map(method -> new Subscriber(subscriber,method))
                .forEach(this::onBindSubscriber);
    }

    private  void onBindSubscriber(Subscriber subcriber){
        // 获取方法注解上的topic
        String topic = subcriber.getSubscribeMethod().getAnnotation(Subscribe.class).value();
        subscriberContainer.computeIfAbsent(topic,key-> new ConcurrentLinkedQueue<>());
        subscriberContainer.get(topic).add(subcriber);
    }

    public void unbind(Object subscriber) {
        Preconditions.checkNotNull(subscriber, "subscriber can't be null!");
        subscriberContainer.forEach((topic,queue)->{
            /*Iterator<Subscriber> iterator = queue.iterator();
            while (iterator.hasNext()){
                Subscriber next = iterator.next();
                if(next.getSubscribeObject() == subscriber){
                    next.setActiveState(false);
                }
            }*/
            queue.stream().forEach(ss -> {
                if(ss.getSubscribeObject() == subscriber){
                    ss.setActiveState(false);
                }
            });
        });
    }

    public ConcurrentLinkedQueue<Subscriber> getTopicSubecribers(String topic){
        return subscriberContainer.get(topic);
    }
}
