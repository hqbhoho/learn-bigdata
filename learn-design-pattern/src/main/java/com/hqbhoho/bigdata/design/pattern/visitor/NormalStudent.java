package com.hqbhoho.bigdata.design.pattern.visitor;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class NormalStudent implements Student {
    private String name;
    private int score;

    public NormalStudent(String name, int score) {
        this.name = name;
        this.score = score;
    }

    @Override
    public void accept(AdmissionOfficer admissionOfficer) {
        admissionOfficer.enrollNormalStudent(this);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getScore() {
        return score;
    }

    @Override
    public int getArtScore() {
        return 0;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "NormalStudent{" +
                "name='" + name + '\'' +
                ", score=" + score +
                '}';
    }
}
