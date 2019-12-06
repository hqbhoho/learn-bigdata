package com.hqbhoho.bigdata.design.pattern.responsibility_chain;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class SchoolMaster extends Leader {
    @Override
    void approveVacation(int dayNum) {
        if(dayNum > 15){
            System.out.println(dayNum + " days,too long,Noone can approve your vacation.");
        }else{
            System.out.println(dayNum + " days,SchoolMaster approve your vacation.");
        }
    }

    @Override
    public String toString() {
        return "SchoolMaster";
    }
}
