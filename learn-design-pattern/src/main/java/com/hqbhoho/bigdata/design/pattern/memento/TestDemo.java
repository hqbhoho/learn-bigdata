package com.hqbhoho.bigdata.design.pattern.memento;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class TestDemo {
    public static void main(String[] args) throws Exception{

        GameRole gameRole = new GameRole();
        StateCaretaker stateCaretaker = new StateCaretaker();
        gameRole.init(200,100);
        System.out.println("===========Init ============");
        System.out.println("game role state: "+ gameRole.getGameRoleState() );
        stateCaretaker.saveSate(gameRole.getGameRoleState());
        gameRole.display();
        System.out.println("===========Fight Boss 1============");
        System.out.println("game role state: "+ gameRole.getGameRoleState() );
        stateCaretaker.saveSate(gameRole.getGameRoleState());
        gameRole.display();
        System.out.println("===========Fight Boss 2============");
        System.out.println("game role state: "+ gameRole.getGameRoleState() );
        stateCaretaker.saveSate(gameRole.getGameRoleState());
        System.out.println("===========After a few time ,state recover Fight Boss 2============");
        gameRole.recover(stateCaretaker,1);
        System.out.println("game role state: "+ gameRole.getGameRoleState() );
        System.out.println("===========After a few time ,state recover Fight Boss 1============");
        gameRole.recover(stateCaretaker,0);
        System.out.println("game role state: "+ gameRole.getGameRoleState() );
    }
}
