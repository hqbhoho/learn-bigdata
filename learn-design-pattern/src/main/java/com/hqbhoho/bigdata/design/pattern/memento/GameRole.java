package com.hqbhoho.bigdata.design.pattern.memento;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class GameRole {
    private GameRoleState gameRoleState;

    public GameRoleState getGameRoleState() {
        return gameRoleState;
    }

    public void init(int blood, int magic){
        this.gameRoleState = new GameRoleState(blood,magic);
    }
    public void display(){
        this.gameRoleState.reduce(20,30);
    }
    public void recover(StateCaretaker stateCaretaker,int index){
        this.gameRoleState = stateCaretaker.getStateAtIndex(index);
    }
}
