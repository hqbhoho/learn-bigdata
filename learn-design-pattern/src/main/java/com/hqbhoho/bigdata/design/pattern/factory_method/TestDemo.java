package com.hqbhoho.bigdata.design.pattern.factory_method;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/29
 */
public class TestDemo {
    public static void main(String[] args) {
        PhoneFactory hw = new HWFactory();
        PhoneFactory xm = new XMFactory();


        Phone phone1 = hw.productPhone("1");
        System.out.println(phone1);
        phone1.call();
        phone1.specialAction();

        System.out.println("=========================================");
        Phone phone2 = hw.productPhone("2");
        System.out.println(phone2);
        phone2.call();
        phone2.specialAction();

        System.out.println("=========================================");
        Phone phone3 = xm.productPhone("1");
        System.out.println(phone3);
        phone3.call();
        phone3.specialAction();

        System.out.println("=========================================");
        Phone phone4 = xm.productPhone("2");
        System.out.println(phone4);
        phone4.call();
        phone4.specialAction();


    }
}
