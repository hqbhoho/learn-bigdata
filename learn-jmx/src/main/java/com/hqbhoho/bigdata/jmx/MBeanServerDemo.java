package com.hqbhoho.bigdata.jmx;

import com.hqbhoho.bigdata.jmx.mbeans.SimpleData;
import com.hqbhoho.bigdata.jmx.mbeans.dynamicMBean.SimpleDataMBean;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/26
 */
public class MBeanServerDemo {
    public static void main(String[] args) throws Exception {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        SimpleData simpleData = new SimpleData();
        SimpleDataMBean simpleDataMBean = new SimpleDataMBean();
        server.registerMBean(simpleData, getObjectName(simpleData));
        server.registerMBean(simpleDataMBean, getObjectName(simpleDataMBean));



        Thread.sleep(Long.MAX_VALUE);

    }

    public static <T> ObjectName getObjectName(T t) throws MalformedObjectNameException {

        return new ObjectName(t.getClass().getPackage().getName() + ":type=" + t.getClass().getSimpleName());
    }
}
