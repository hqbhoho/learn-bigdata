package com.hqbhoho.bigdata.test;



import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/22
 */
public class Test {

    static class Person{
    }

    static class User extends Person{}



    public static void main(String[] args) throws Exception {
//        List<String> paths = FileUtils.traversalFileDirectory(new File("C:\\Users\\13638\\Desktop\\MOT测试部署"));

//        paths.stream().forEach(System.out::println);
        System.out.println(LocalDate.now().atStartOfDay().toEpochSecond(ZoneOffset.of("+8")));

        User u = new User();
        Person p = u;

        System.out.println(u == p);



    }
}
