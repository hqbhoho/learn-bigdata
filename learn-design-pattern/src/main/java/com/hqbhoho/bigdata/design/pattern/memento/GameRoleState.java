package com.hqbhoho.bigdata.design.pattern.memento;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class GameRoleState implements Cloneable{
    private int blood;
    private int magic;

    public GameRoleState(int blood, int magic) {
        this.blood = blood;
        this.magic = magic;
    }

    public int getBlood() {
        return blood;
    }

    public void setBlood(int blood) {
        this.blood = blood;
    }

    public int getMagic() {
        return magic;
    }

    public void setMagic(int magic) {
        this.magic = magic;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        return "GameRoleState{" +
                "blood=" + blood +
                ", magic=" + magic +
                '}';
    }

    public void reduce(int i, int i1) {
        this.blood -= i;
        this.magic -= i1;
    }
}
