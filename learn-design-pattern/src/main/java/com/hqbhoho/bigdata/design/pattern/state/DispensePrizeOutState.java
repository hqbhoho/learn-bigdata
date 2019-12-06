package com.hqbhoho.bigdata.design.pattern.state;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class DispensePrizeOutState implements RaffleState {
    private RaffleActivity raffleActivity;

    public DispensePrizeOutState(RaffleActivity raffleActivity) {
        this.raffleActivity = raffleActivity;
    }

    @Override
    public void deductCredit() {
    }

    @Override
    public boolean raffle() {
        return false;
    }

    @Override
    public void dispensePrize() {
    }
}
