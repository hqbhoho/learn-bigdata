package com.hqbhoho.bigdata.design.pattern.state;

/**
 * 抽奖状态
 */
public interface RaffleState {
    // 扣除积分，准备参加抽奖
    void deductCredit();
    // 抽奖
    boolean raffle();
    // 发放奖品
    void dispensePrize();

}
