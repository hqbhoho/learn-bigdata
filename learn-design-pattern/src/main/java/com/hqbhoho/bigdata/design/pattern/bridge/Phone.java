package com.hqbhoho.bigdata.design.pattern.bridge;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public interface Phone {
    void installSoftware(Software software);
    void listSoftware();
    void showMe();
    void showBrand();
}
