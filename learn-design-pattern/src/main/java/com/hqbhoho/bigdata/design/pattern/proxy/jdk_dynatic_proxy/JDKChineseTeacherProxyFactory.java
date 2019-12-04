package com.hqbhoho.bigdata.design.pattern.proxy.jdk_dynatic_proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class JDKChineseTeacherProxyFactory implements InvocationHandler{

    private Teacher teacher;

    public JDKChineseTeacherProxyFactory() {
        this.teacher = new ChineseTeacher();
    }

    public Object newProxyInstance() {

        return Proxy.newProxyInstance(teacher.getClass().getClassLoader(),
                teacher.getClass().getInterfaces(),
                this
        );
    }

    @Override
    public Object invoke(Object o, Method method, Object[] objects) throws Throwable {

        if("teach".equalsIgnoreCase(method.getName())){
            preTeach();
            method.invoke(teacher,objects);
            postTeach();
        }
        return null;
    }

    private void preTeach() {
        System.out.println("我是代理老师,现在给你们上课...<JDK_proxy>");
    }

    private void postTeach() {
        System.out.println("我是代理老师,现在下课...<JDK_proxy>");
    }

}
