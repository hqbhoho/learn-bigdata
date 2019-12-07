package com.hqbhoho.bigdata.design.pattern.visitor;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public interface AdmissionOfficer {
    boolean enrollNormalStudent(Student normalstudent);
    boolean enrollArtStudent(Student artstudent);
}
