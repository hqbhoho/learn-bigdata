package com.hqbhoho.bigdata.design.pattern.prototype.deep_with_object_stream;

import java.io.*;

/**
 * describe:
 * <p>
 * 对象输入输出流  完成对象的深拷贝
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/28
 */
public class Person implements Cloneable, Serializable {
    private String name;
    private int age;
    private Car car;

    @Override
    protected Object clone() throws CloneNotSupportedException {
        Object result = null;
        ByteArrayInputStream bais = null;
        ObjectInputStream ois = null;

        try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
        ) {
            oos.writeObject(this);
            oos.flush();
            bais = new ByteArrayInputStream(baos.toByteArray());
            ois = new ObjectInputStream(bais);
            result = ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bais != null) {
                try {
                    bais.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        return result;
    }

    public Person(String name, int age, Car car) {
        this.name = name;
        this.age = age;
        this.car = car;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Car getCar() {
        return car;
    }

    public void setCar(Car car) {
        this.car = car;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", car=" + car +
                '}';
    }
}
