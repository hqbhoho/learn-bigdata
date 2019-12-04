package com.hqbhoho.bigdata.design.pattern.decorator;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/03
 */
public class TestDemo {
    public static void main(String[] args) {
        System.out.println("=======HW裸机=========");
        HWPhone hwPhone = new HWPhone(3999.9);
        System.out.println(hwPhone.getDetails());
        System.out.println(hwPhone.getPrice());
        System.out.println("=======HW裸机+手机壳=========");
        ShellDecoratorPhone hwShellDecoratorPhone = new ShellDecoratorPhone(hwPhone, 50.0);
        System.out.println(hwShellDecoratorPhone.getDetails());
        System.out.println(hwShellDecoratorPhone.getPrice());
        System.out.println("=======HW裸机+手机壳+屏保=========");
        ScreenProtectedDecoratorPhone hwShellScreenDecoratorPhone = new ScreenProtectedDecoratorPhone(hwShellDecoratorPhone, 100.0);
        System.out.println(hwShellScreenDecoratorPhone.getDetails());
        System.out.println(hwShellScreenDecoratorPhone.getPrice());
        System.out.println("=======XM裸机=========");
        HWPhone xmPhone = new HWPhone(1999.9);
        System.out.println(xmPhone.getDetails());
        System.out.println(xmPhone.getPrice());
        System.out.println("=======XM裸机+手机壳=========");
        ShellDecoratorPhone xmShellDecoratorPhone = new ShellDecoratorPhone(xmPhone, 50.0);
        System.out.println(xmShellDecoratorPhone.getDetails());
        System.out.println(xmShellDecoratorPhone.getPrice());
        System.out.println("=======XM裸机+手机壳+屏保=========");
        ScreenProtectedDecoratorPhone xmShellScreenDecoratorPhone = new ScreenProtectedDecoratorPhone(xmShellDecoratorPhone, 100.0);
        System.out.println(xmShellScreenDecoratorPhone.getDetails());
        System.out.println(xmShellScreenDecoratorPhone.getPrice());
    }
}
