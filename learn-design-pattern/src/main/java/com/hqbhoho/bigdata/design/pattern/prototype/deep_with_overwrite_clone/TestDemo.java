package com.hqbhoho.bigdata.design.pattern.prototype.deep_with_overwrite_clone;

/**
 * describe:
 *
 * 深拷贝，引用对象  不指向同一个对象
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/28
 */
public class TestDemo {
    public static void main(String[] args) throws Exception{
        Car car = new Car("haha", 3);

        Person p1 = new Person("hqbhoho", 1, car);
        Person p2 = (Person) p1.clone();
        Person p3 = (Person) p1.clone();
        Person p4 = (Person) p1.clone();
        Person p5 = (Person) p1.clone();

        car.setAge(5);

        System.out.println("p1： " + p1 + "===========" + p1.getCar().hashCode());
        System.out.println("p1： " + p2 + "===========" + p2.getCar().hashCode());
        System.out.println("p1： " + p3 + "===========" + p3.getCar().hashCode());
        System.out.println("p1： " + p4 + "===========" + p4.getCar().hashCode());
        System.out.println("p1： " + p5 + "===========" + p5.getCar().hashCode());
    }
}
