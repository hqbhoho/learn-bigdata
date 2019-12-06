package com.hqbhoho.bigdata.design.pattern.state;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class NoRaffleState implements RaffleState {

    private RaffleActivity raffleActivity;

    public NoRaffleState(RaffleActivity raffleActivity) {
        this.raffleActivity = raffleActivity;
    }

    @Override
    public void deductCredit() {
        System.out.println("deduct 30 credit,you can join raffle activity");
        this.raffleActivity.setCurrentRaffleState(this.raffleActivity.getCanRaffleState());
    }

    @Override
    public boolean raffle() {
        System.out.println("you have not join raffle activity,you can't raffle");
        return false;
    }

    @Override
    public void dispensePrize() {
        System.out.println("you have not join raffle activity,you can't dispense prize");
    }
}
