package com.hqbhoho.bigdata.design.pattern.proxy.cglib_dynatic_proxy;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class CglibChineseTeacherProxyFactory implements MethodInterceptor {

    private ChineseTeacher chineseTeacher ;

    public CglibChineseTeacherProxyFactory() {
        this.chineseTeacher = new ChineseTeacher();
    }

    public Object newProxyInstance(){
        Enhancer enhancer = new Enhancer();
        //这一步就是告诉cglib，生成的子类需要继承哪个类
        enhancer.setSuperclass(chineseTeacher.getClass());
        enhancer.setCallback(this);
        Object proxyObj = enhancer.create();
        // 返回代理对象
        //第一步、生成源代码
        //第二步、编译成class文件
        //第三步、加载到JVM中，并返回被代理对象
        return proxyObj;
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        if("teach".equalsIgnoreCase(method.getName())){
            preTeach();
            method.invoke(chineseTeacher,args);
            postTeach();
        }
        return null;
    }

    private void preTeach() {
        System.out.println("我是代理老师,现在给你们上课...<cglib_proxy>");
    }

    private void postTeach() {
        System.out.println("我是代理老师,现在下课...<cglib_proxy>");
    }
}
