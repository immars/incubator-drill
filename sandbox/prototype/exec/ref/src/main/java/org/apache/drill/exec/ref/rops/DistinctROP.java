package org.apache.drill.exec.ref.rops;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.common.logical.data.Distinct;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.ref.eval.EvaluatorTypes;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.RecordPointer;

import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-25
 * Time: 上午12:21
 * To change this template use File | Settings | File Templates.
 */
public class DistinctROP extends SingleInputROPBase<Distinct> {
    private static Log logger = LogFactory.getLog(DistinctROP.class);


    private Set<Integer> duplicate = new HashSet<Integer>();
    private FieldReference ref;
    private EvaluatorTypes.BasicEvaluator evaluator;
    private DistictIter iter;

    public DistinctROP(Distinct config) {
        super(config);
        this.ref = config.getRef();
    }

    @Override
    protected void setupEvals(EvaluatorFactory builder) throws SetupException {
        evaluator = builder.getBasicEvaluator(record, ref);
    }

    @Override
    protected void setInput(RecordIterator incoming) {
        this.iter = new DistictIter(incoming);
    }

    @Override
    protected RecordIterator getIteratorInternal() {
        return iter;
    }

    private class DistictIter implements RecordIterator {
        RecordIterator incoming;

        private DistictIter(RecordIterator incoming) {
            this.incoming = incoming;
        }

        @Override
        public RecordPointer getRecordPointer() {
            return record;
        }

        @Override
        public NextOutcome next() {
            NextOutcome r;
            while (true) {
                r = incoming.next();
                if (r == NextOutcome.NONE_LEFT) return NextOutcome.NONE_LEFT;
                DataValue dv = evaluator.eval();

                if (!duplicate.contains(dv.hashCode())) {
                    duplicate.add(dv.hashCode());
                    return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
                }
            }
        }

        @Override
        public ROP getParent() {
            return DistinctROP.this;
        }
    }
}
