package com.hqbhoho.bigdata.design.pattern.responsibility_chain;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class TestDemo {
    public static void main(String[] args) {

        Leader teacherMaster = new TeacherMaster();
        Leader collegeMaster = new CollegeMaster();
        Leader schoolMaster = new SchoolMaster();


        collegeMaster.setNext(schoolMaster);
        teacherMaster.setNext(collegeMaster);

        teacherMaster.approveVacation(3);
        System.out.println("==============================");
        teacherMaster.approveVacation(7);
        System.out.println("==============================");
        teacherMaster.approveVacation(12);
        System.out.println("==============================");
        teacherMaster.approveVacation(17);

    }
}
