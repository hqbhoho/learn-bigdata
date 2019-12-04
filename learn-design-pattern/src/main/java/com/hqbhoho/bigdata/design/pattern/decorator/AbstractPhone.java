package com.hqbhoho.bigdata.design.pattern.decorator;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/03
 */
public abstract class AbstractPhone implements Phone {

    private String details;
    private Double price;

    public AbstractPhone() {
    }

    public AbstractPhone(String details, Double price) {
        this.details = details;
        this.price = price;
    }

    @Override
    public String getDetails() {
        return details;
    }

    @Override
    public double getPrice() {
        return price;
    }
}
