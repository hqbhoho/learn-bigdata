package com.hqbhoho.bigdata.guava.eventbus.customer;

import java.lang.reflect.Method;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/10
 */
class Subscriber {

    private Object subscribeObject;
    private Method subscribeMethod;
    private boolean activeState = true;

    public Subscriber(Object subscribeObject, Method subscribeMethod) {
        this.subscribeObject = subscribeObject;
        this.subscribeMethod = subscribeMethod;
    }

    public Object getSubscribeObject() {
        return subscribeObject;
    }

    public void setSubscribeObject(Object subscribeObject) {
        this.subscribeObject = subscribeObject;
    }

    public Method getSubscribeMethod() {
        return subscribeMethod;
    }

    public void setSubscribeMethod(Method subscribeMethod) {
        this.subscribeMethod = subscribeMethod;
    }

    public boolean isActiveState() {
        return activeState;
    }

    public void setActiveState(boolean activeState) {
        this.activeState = activeState;
    }

    @Override
    public String toString() {
        return "Subscriber{" +
                "subscribeObject=" + subscribeObject +
                ", subscribeMethod=" + subscribeMethod +
                '}';
    }
}
