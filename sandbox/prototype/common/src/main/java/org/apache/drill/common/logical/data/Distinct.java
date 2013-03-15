package org.apache.drill.common.logical.data;


import org.apache.drill.common.expression.FieldReference;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-25
 * Time: 上午12:16
 * To change this template use File | Settings | File Templates.
 */
public class Distinct extends SingleInputOperator {
    private FieldReference ref;

    public Distinct(FieldReference ref) {
        this.ref = ref;
    }

    public FieldReference getRef() {
        return ref;
    }
}
