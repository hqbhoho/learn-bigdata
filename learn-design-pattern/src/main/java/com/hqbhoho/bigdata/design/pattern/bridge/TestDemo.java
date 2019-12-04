package com.hqbhoho.bigdata.design.pattern.bridge;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class TestDemo {
    public static void main(String[] args) {
        Phone hwPhone = new HWPhone();
        Phone xmPhone = new XMPhone();

        hwPhone.installSoftware(new QQ());
        hwPhone.installSoftware(new Twitter());

        xmPhone.installSoftware(new Wechat());

        hwPhone.showMe();
        System.out.println("==========================");
        xmPhone.showMe();


    }
}
