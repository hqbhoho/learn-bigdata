package com.hqbhoho.bigdata.design.pattern.facade;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/03
 */
public class Computer {
    private Cpu cpu;
    private Ram Ram;
    private Ssd ssd;
    private Screen screen;

    public Computer() {
        this.cpu =new Cpu();
        Ram = new Ram();
        this.ssd = new Ssd();
        this.screen = new Screen();
    }

    public void start(){
        System.out.println("Turn on Computer......");
        this.cpu.onStart();
        this.ssd.onStart();
        this.Ram.onStart();
        this.screen.onStart();
    }

    public void stop(){
        System.out.println("Turn off Computer......");
        this.screen.onStop();
        this.Ram.onStop();
        this.ssd.onStop();
        this.cpu.onStop();
    }
}
