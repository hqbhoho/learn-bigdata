package com.hqbhoho.bigdata.design.pattern.template_method;

public abstract class Game {
    protected abstract void open();
    protected void initialize(){
        System.out.println("Game initialize......");
    }
    protected void readHistory(){
        System.out.println("Game readHistory......");
    };
    protected abstract void startPlay();
    protected abstract void stopPlay();
    protected abstract boolean canReadHistory();
    public final void  playGame(){
        open();
        initialize();
        if(canReadHistory()){
            readHistory();
        }
        startPlay();
        stopPlay();
    };
}
