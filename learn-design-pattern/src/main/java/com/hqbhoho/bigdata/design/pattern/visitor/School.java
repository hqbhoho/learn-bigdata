package com.hqbhoho.bigdata.design.pattern.visitor;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class School {
    private List<Student> students;

    public School() {
        this.students = new ArrayList<>();
        System.out.println("Enroll Student Standard:\n Normal Student: score > 600 \n Art Student: score > 400 and artScore > 80");
    }

    public void add(Student student){
        this.students.add(student);
    }

    public void enrollNewStudent(AdmissionOfficer admissionOfficer){
        students.stream().forEach(student ->{
                    boolean flag = false;
                    System.out.println("============================================");
                    System.out.println("student: "+student);
                    if(student instanceof NormalStudent){
                        flag =  admissionOfficer.enrollNormalStudent(student);
                    }
                    if(student instanceof  ArtStudent){
                        flag = admissionOfficer.enrollArtStudent(student);
                    }
                    if(flag) System.out.println("success!!!"); else System.out.println("fail!!!");
                }
        );
    }
}
