package com.hqbhoho.bigdata.design.pattern.decorator;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/03
 */
public class ScreenProtectedDecoratorPhone extends DecoratorPhone {
    public ScreenProtectedDecoratorPhone(Phone phone,Double price){
        super("add Phone ScreenProtected",price,phone);
    }
}
