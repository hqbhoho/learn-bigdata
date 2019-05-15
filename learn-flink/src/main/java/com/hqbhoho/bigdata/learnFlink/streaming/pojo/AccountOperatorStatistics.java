package com.hqbhoho.bigdata.learnFlink.streaming.pojo;

/**
 * describe:
 *
 * 账户操作统计
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/10
 */
public class AccountOperatorStatistics {

    private String Account;
//    private Integer AccountOperatorCount;
    private Integer AccountOperatorInCount = 0;
    private Integer AccountOperatorOutCount = 0;
    private Integer AccountMoneyCount;

    public AccountOperatorStatistics() {
    }

    public AccountOperatorStatistics(String account, Integer accountOperatorInCount, Integer accountOperatorOutCount, Integer accountMoneyCount) {
        Account = account;
        AccountOperatorInCount = accountOperatorInCount;
        AccountOperatorOutCount = accountOperatorOutCount;
        AccountMoneyCount = accountMoneyCount;
    }

    public String getAccount() {
        return Account;
    }

    public void setAccount(String account) {
        Account = account;
    }

    public Integer getAccountOperatorInCount() {
        return AccountOperatorInCount;
    }

    public void setAccountOperatorInCount(Integer accountOperatorInCount) {
        AccountOperatorInCount = accountOperatorInCount;
    }

    public Integer getAccountOperatorOutCount() {
        return AccountOperatorOutCount;
    }

    public void setAccountOperatorOutCount(Integer accountOperatorOutCount) {
        AccountOperatorOutCount = accountOperatorOutCount;
    }

    public Integer getAccountMoneyCount() {
        return AccountMoneyCount;
    }

    public void setAccountMoneyCount(Integer accountMoneyCount) {
        AccountMoneyCount = accountMoneyCount;
    }

    @Override
    public String toString() {
        return "AccountOperatorStatistics{" +
                "Account='" + Account + '\'' +
                ", AccountOperatorInCount=" + AccountOperatorInCount +
                ", AccountOperatorOutCount=" + AccountOperatorOutCount +
                ", AccountMoneyCount=" + AccountMoneyCount +
                '}';
    }

    public static AccountOperatorStatistics of(String account, Integer accountOperatorInCount, Integer accountOperatorOutCount, Integer accountMoneyCount){
        return new AccountOperatorStatistics(account,accountOperatorInCount,accountOperatorOutCount, accountMoneyCount);
    }
}
