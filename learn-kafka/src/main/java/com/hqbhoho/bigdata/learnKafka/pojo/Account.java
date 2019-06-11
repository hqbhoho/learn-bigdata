package com.hqbhoho.bigdata.learnKafka.pojo;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/10
 */
public class Account {
    private String name;
    private Double account;
    private Long eventTime;

    public Account() {
    }

    public Account(String name, Double account, Long eventTime) {
        this.name = name;
        this.account = account;
        this.eventTime = eventTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getAccount() {
        return account;
    }

    public void setAccount(Double account) {
        this.account = account;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}
