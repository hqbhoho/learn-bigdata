package com.hqbhoho.bigdata.design.pattern.visitor;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class NormalAdmissionOfficer implements AdmissionOfficer {
    @Override
    public boolean enrollNormalStudent(Student normalstudent) {
        return normalstudent.getScore() > 600;
    }

    @Override
    public boolean enrollArtStudent(Student artstudent) {
        System.out.println("I can't enroll art Student...");
        return false;
    }
}
