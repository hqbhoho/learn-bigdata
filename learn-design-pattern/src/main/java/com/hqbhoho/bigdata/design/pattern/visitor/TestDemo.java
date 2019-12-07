package com.hqbhoho.bigdata.design.pattern.visitor;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class TestDemo {
    public static void main(String[] args) {
        School school = new School();
        school.add(new NormalStudent("hqbhoho01",550));
        school.add(new NormalStudent("hqbhoho02",650));
        school.add(new ArtStudent("hqbhoho03",350,100));
        school.add(new ArtStudent("hqbhoho03",450,60));
        school.add(new ArtStudent("hqbhoho03",450,100));
        System.out.println("================Normal=================================");
        school.enrollNewStudent(new NormalAdmissionOfficer());
        System.out.println("================Art=================================");
        school.enrollNewStudent(new ArtAdmissionOfficer());
    }
}
