package com.hqbhoho.bigdata.design.pattern.visitor;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class ArtAdmissionOfficer implements AdmissionOfficer {
    @Override
    public boolean enrollNormalStudent(Student normalstudent) {
        System.out.println("I can't enroll normal Student...");
        return false;
    }

    @Override
    public boolean enrollArtStudent(Student artstudent) {
        return artstudent.getScore() > 400 && artstudent.getArtScore() >80;
    }
}
