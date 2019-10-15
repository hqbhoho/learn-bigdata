package com.hqbhoho.bigdata.guava.cache;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/15
 */
public class Student {
    private String Name;
    private String id;
//    private byte[] data = new byte[1024 * 1024];

    public Student(String name, String id) {
        Name = name;
        this.id = id;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    protected void finalize() throws Throwable {
        System.out.println(this + "will be collected...");
    }

    @Override
    public String toString() {
        return "Student{" +
                "Name='" + Name + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
