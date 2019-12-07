package com.hqbhoho.bigdata.design.pattern.interpreter;

import java.util.HashMap;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class AddExpression extends SymbolExpression  {

    public AddExpression(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public int interpreter(HashMap<String, Integer> map) {
        return super.getLeft().interpreter(map) + super.getRight().interpreter(map);
    }
}
