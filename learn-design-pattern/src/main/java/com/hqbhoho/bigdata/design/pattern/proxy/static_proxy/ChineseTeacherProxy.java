package com.hqbhoho.bigdata.design.pattern.proxy.static_proxy;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class ChineseTeacherProxy implements Teacher {
    private Teacher teacher;

    public ChineseTeacherProxy() {
        this.teacher = new ChineseTeacher();
    }

    private void preTeach() {
        System.out.println("我是代理老师,现在给你们上课...");
    }

    @Override
    public void teach() {
        preTeach();
        teacher.teach();
        postTeach();
    }

    private void postTeach() {
        System.out.println("我是代理老师,现在下课...");
    }
}
