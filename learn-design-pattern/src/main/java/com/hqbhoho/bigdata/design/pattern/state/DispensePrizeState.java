package com.hqbhoho.bigdata.design.pattern.state;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class DispensePrizeState implements RaffleState {
    private RaffleActivity raffleActivity;

    public DispensePrizeState(RaffleActivity raffleActivity) {
        this.raffleActivity = raffleActivity;
    }

    @Override
    public void deductCredit() {
    }

    @Override
    public boolean raffle() {
        return  false;
    }

    @Override
    public void dispensePrize() {
        if(this.raffleActivity.getCount() > 1){
            System.out.println("dispense prize......");
            this.raffleActivity.setCurrentRaffleState(this.raffleActivity.getNoRaffleState());
        }else if (this.raffleActivity.getCount() == 1) {
            System.out.println("dispense prize......");
            this.raffleActivity.setCurrentRaffleState(this.raffleActivity.getDispensePrizeOutState());
        }
        this.raffleActivity.setCount(this.raffleActivity.getCount()-1);
    }
}
