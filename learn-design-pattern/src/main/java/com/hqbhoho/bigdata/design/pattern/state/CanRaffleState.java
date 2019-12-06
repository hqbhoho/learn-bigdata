package com.hqbhoho.bigdata.design.pattern.state;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class CanRaffleState implements RaffleState {

    private RaffleActivity raffleActivity;

    public CanRaffleState(RaffleActivity raffleActivity) {
        this.raffleActivity = raffleActivity;
    }

    @Override
    public void deductCredit() {
        System.out.println("you have deducted 30 credit,you can raffle ");
    }

    @Override
    public boolean raffle() {
        boolean flag = false;
        int raffleRes = (int)(1+Math.random()*(10-1+1));
        System.out.println("you raffle num :" +raffleRes);
        if(raffleRes == 6){
            System.out.println("you have Winning the prize....");
            this.raffleActivity.setCurrentRaffleState(this.raffleActivity.getDispensePrizeState());
            flag = true;
        } else{
            System.out.println("you have not Winning the prize");
            this.raffleActivity.setCurrentRaffleState(this.raffleActivity.getNoRaffleState());
        }
        return flag;
    }

    @Override
    public void dispensePrize() {
        System.out.println("you have not Winning the prize,you can't dispense prize");
    }
}
