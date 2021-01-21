package com.dremio.nessie.versioned.store.rocksdb;

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.ValueType;

public class ConditionExecutor {
  public <C extends BaseValue<C>> boolean evaluateCondition(SaveOp<C> saveOp, Condition condition) {
    if (saveOp.getType() == ValueType.L1) {
      l1Evaluator.evaluate(saveOp, condition);
    }
    return true;
  }

  public abstract class Evaluator<C extends BaseValue<C>> {
    public abstract boolean evaluate(SaveOp<C> saveOp, Condition condition);
  }

  final class L1Evaluator<L1 extends BaseValue<L1>> extends Evaluator<L1> {
    @Override
    public boolean evaluate(SaveOp<L1> saveOp, Condition condition) {
      return false;
    }
  }

  final L1Evaluator l1Evaluator = new L1Evaluator();
}
