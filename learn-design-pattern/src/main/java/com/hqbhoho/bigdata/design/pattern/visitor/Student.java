package com.hqbhoho.bigdata.design.pattern.visitor;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public interface Student {
    int getScore();
    int getArtScore();
    void accept(AdmissionOfficer admissionOfficer);
}
