package com.hqbhoho.bigdata.design.pattern.bridge;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class HWPhone extends AbstractPhone {
    @Override
    public void showBrand() {
        System.out.println("Brand: HuaWei");
    }
}
