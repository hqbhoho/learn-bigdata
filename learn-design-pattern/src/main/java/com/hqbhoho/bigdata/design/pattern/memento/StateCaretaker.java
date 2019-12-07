package com.hqbhoho.bigdata.design.pattern.memento;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class StateCaretaker {
    private List<GameRoleState> states = new ArrayList<>();

    public void saveSate(GameRoleState gameRoleState)throws Exception {
        this.states.add((GameRoleState) gameRoleState.clone());
    }

    public GameRoleState getStateAtIndex(int index) {
        return this.states.get(index);
    }

}
