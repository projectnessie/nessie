package com.dremio.nessie.versioned.impl;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.rocksdb.Condition;
import com.dremio.nessie.versioned.store.rocksdb.Function;
import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.tiered.builder.Ref;

public class ConditionExecutor {
  public <C extends BaseValue<C>> boolean evaluateCondition(C value, Condition condition) {
    if (value instanceof L1) {
      l1Evaluator.evaluate(value, condition);
    } else if (value instanceof Ref) {
      refEvaluator.evaluate(value, condition);
    }
    return true;
  }

  interface Evaluator<C extends BaseValue<C>> {
    public boolean evaluate(C saveOp, Condition condition);
  }

  // Within RocksDBStore we call
  // final L1 read = EntityType.L1.loadSingle(RocksDBStore, internalL1.getId());
  // l1Evaluator.evaluate(read, condition);
  final class L1Evaluator<L1 extends BaseValue<L1>> implements Evaluator<L1> {
    @Override
    public boolean evaluate(L1 value, Condition condition) {
      for (Function function: condition.functionList) {
        // Retrieve entity at function.path
        // TODO: Only InternalL1 has access to get internals, which are needed.
        InternalL1 internalL1 = InternalL1.EMPTY;
        List<String> elements = Arrays.asList(function.getPath().split(Pattern.quote(".")));

      }
      return false;
    }
  }

  final class RefEvaluator<Ref extends BaseValue<Ref>> implements Evaluator<Ref> {
    @Override
    public boolean evaluate(Ref value, Condition condition) {
      boolean result = true;
      // Retrieve entity at function.path
      // TODO: This is a workaround until I figure out how to get InternalRef from deserialized data.
      InternalRef internalRef = EntityType.REF.buildEntity(producer -> producer.id(Id.build("fred"))
          .type(com.dremio.nessie.tiered.builder.Ref.RefType.BRANCH));
      if (internalRef.getType() == InternalRef.Type.BRANCH) {
        InternalBranch internalBranch = internalRef.getBranch();
        for (Function function: condition.functionList) {
          // Branch evaluation
          List<String> path = Arrays.asList(function.getPath().split(Pattern.quote(".")));

          for (String segment : path) {
            if (segment.equals("type")
                && (path.size() == 1)
                && (function.getOperator().equals(Function.EQUALS))
                && (internalBranch.getType().toString() != function.getValue().getString())) {
              return false;
            } else if (segment.equals("children")) {
              // Is a Stream

            } else if (segment.equals("metadata")
                && (path.size() == 1)
                && (function.getOperator().equals(Function.EQUALS))) {
              // TODO: We require a getMetadata() accessor in InternalBranch
              return false;
            } else if (segment.equals(("commits"))) {
              if (function.getOperator().equals(Function.SIZE)) {
                // Is a List
                // TODO: We require a getCommits() accessor in InternalBranch
                return false;
              } else if (function.getOperator().equals(Function.EQUALS)) {
                // TODO: We require a getCommits() accessor in InternalBranch
              }
            }
          }
        }

        // Tag evaluation
        if (internalRef.getType() == InternalRef.Type.TAG) {
          InternalTag internalTag = internalRef.getTag();
          for (Function function: condition.functionList) {
            // Tag evaluation
            List<String> path = Arrays.asList(function.getPath().split(Pattern.quote(".")));

            for (String segment : path) {
              if (segment.equals("type")
                && (path.size() == 1)
                && (function.getOperator().equals(Function.EQUALS))
                && (internalTag.getType().toString() != function.getValue().getString())) {
                return false;
              } else if (segment.equals(("commit"))) {
                if (function.getOperator().equals(Function.EQUALS)) {
                  if (!internalTag.getCommit().equals(function.getValue().getBinary()))
                  // TODO: We require a getCommit() accessor in internalTag
                  return false;
                }
              }
            }

        }
      }
      return result;
    }
  }

  final L1Evaluator l1Evaluator = new L1Evaluator();
  final RefEvaluator refEvaluator = new RefEvaluator();
}
