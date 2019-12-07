package com.hqbhoho.bigdata.design.pattern.visitor;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class ArtStudent implements Student {
    private String name;
    private int score;
    private int artScore;

    public ArtStudent(String name, int score, int artScore) {
        this.name = name;
        this.score = score;
        this.artScore = artScore;
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

    public void setScore(int score) {
        this.score = score;
    }

    public int getArtScore() {
        return artScore;
    }

    public void setArtScore(int artScore) {
        this.artScore = artScore;
    }

    @Override
    public String toString() {
        return "ArtStudent{" +
                "name='" + name + '\'' +
                ", score=" + score +
                ", artScore=" + artScore +
                '}';
    }

    @Override
    public void accept(AdmissionOfficer admissionOfficer) {
        admissionOfficer.enrollArtStudent(this);
    }
}
