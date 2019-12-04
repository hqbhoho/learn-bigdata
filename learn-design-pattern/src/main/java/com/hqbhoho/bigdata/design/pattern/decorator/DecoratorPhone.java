package com.hqbhoho.bigdata.design.pattern.decorator;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/03
 */
public abstract class DecoratorPhone implements Phone{
    private String details;
    private Double price;
    private Phone phone;

    public DecoratorPhone() {
    }

    public DecoratorPhone(String details, Double price, Phone phone) {
        this.details = details;
        this.price = price;
        this.phone = phone;
    }

    @Override
    public String getDetails() {
        return phone.getDetails() + "<===>" + details;
    }

    @Override
    public double getPrice() {
        return phone.getPrice() + price;
    }

}
