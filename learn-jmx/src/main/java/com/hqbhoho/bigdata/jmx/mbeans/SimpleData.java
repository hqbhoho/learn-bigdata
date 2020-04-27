package com.hqbhoho.bigdata.jmx.mbeans;

import javax.management.*;
import java.util.concurrent.atomic.AtomicLong;

import static javax.management.AttributeChangeNotification.ATTRIBUTE_CHANGE;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/26
 */
public class SimpleData extends NotificationBroadcasterSupport
        implements SimpleDataMBean, NotificationFilter, NotificationListener {

    private String data;

    private static final AtomicLong num = new AtomicLong(0l);

    public SimpleData() {
        this.addNotificationListener(this, this, new CallBack());
    }

    @Override
    public void setData(String data) {
        String oldValue = this.data;
        this.data = data;
        Notification notification = new AttributeChangeNotification(
                this,
                num.incrementAndGet(),
                System.currentTimeMillis(),
                "Value change oldValue: " + oldValue + ",newValue: " + this.data,
                "data",
                ATTRIBUTE_CHANGE,
                oldValue,
                this.data);

        sendNotification(notification);

    }

    @Override
    public String getData() {
        return this.data;
    }

    @Override
    public String display() {
        return this.data;
    }

    @Override
    public boolean isNotificationEnabled(Notification notification) {
        if (notification.getType().equalsIgnoreCase(ATTRIBUTE_CHANGE)) {
            return true;
        }
        return false;
    }

    @Override
    public void handleNotification(Notification notification, Object o) {
        ((CallBack) o).handle(notification);
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        return new MBeanNotificationInfo[]{
                new MBeanNotificationInfo(new String[]{ATTRIBUTE_CHANGE},"data change","ATTRIBUTE_CHANGE")
        };
    }


    static class CallBack {
        public void handle(Notification notification) {
            System.out.println(notification);
        }
    }
}
