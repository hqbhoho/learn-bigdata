package com.hqbhoho.bigdata.design.pattern.state;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class TestDemo {
    public static void main(String[] args) throws InterruptedException {

        RaffleActivity raffleActivity = new RaffleActivity(10);
        int index = 1;
        System.out.println("==============raffle number: 6===================");
        while(true){
            System.out.println("==============raffle "+index+" ===================");
            if(!raffleActivity.raffle()) break;
            index++;
        }
        System.out.println("raffle count: "+ (index-1) +",prize count: 10....");

    }

}
