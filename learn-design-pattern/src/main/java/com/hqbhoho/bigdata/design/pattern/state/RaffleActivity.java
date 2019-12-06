package com.hqbhoho.bigdata.design.pattern.state;

/**
 * describe:
 *  抽奖活动
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class RaffleActivity {

    private RaffleState currentRaffleState;

    // 奖品数
    private int count;

    private NoRaffleState noRaffleState;
    private CanRaffleState canRaffleState;
    private DispensePrizeState dispensePrizeState;
    private DispensePrizeOutState dispensePrizeOutState;

    public RaffleActivity(int count) {
        this.count = count;
        this.noRaffleState = new NoRaffleState(this);
        this.canRaffleState = new CanRaffleState(this);
        this.dispensePrizeState = new DispensePrizeState(this);
        this.dispensePrizeOutState = new DispensePrizeOutState(this);
        this.currentRaffleState = noRaffleState;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public NoRaffleState getNoRaffleState() {
        return noRaffleState;
    }

    public CanRaffleState getCanRaffleState() {
        return canRaffleState;
    }

    public DispensePrizeState getDispensePrizeState() {
        return dispensePrizeState;
    }

    public DispensePrizeOutState getDispensePrizeOutState() {
        return dispensePrizeOutState;
    }

    public void setCurrentRaffleState(RaffleState currentRaffleState) {
        this.currentRaffleState = currentRaffleState;
    }

    // 抽奖
    public boolean raffle(){
        if(this.currentRaffleState == dispensePrizeOutState){
            System.out.println("prize num : 0 , raffle activity is over....");
            return false;
        }
        this.currentRaffleState.deductCredit();
        if(this.currentRaffleState.raffle()) this.currentRaffleState.dispensePrize();
        return true;
    }
}
