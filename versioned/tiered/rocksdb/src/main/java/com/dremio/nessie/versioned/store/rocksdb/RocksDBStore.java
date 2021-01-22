package com.dremio.nessie.versioned.store.rocksdb;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;

public class RocksDBStore implements Store {
  @Override
  public void start() {

  }

  @Override
  public void close() {

  }

  @Override
  public void load(LoadStep loadstep) {

  }

  @Override
  public <C extends BaseValue<C>> boolean putIfAbsent(SaveOp<C> saveOp) {
    return false;
  }

  @Override
  public <C extends BaseValue<C>> void put(SaveOp<C> saveOp, Optional<ConditionExpression> condition) {

  }

  @Override
  public <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id, Optional<ConditionExpression> condition) {
    return false;
  }

  @Override
  public void save(List<SaveOp<?>> ops) {

  }

  @Override
  public <C extends BaseValue<C>> void loadSingle(ValueType<C> type, Id id, C consumer) {

  }

  @Override
  public <C extends BaseValue<C>> boolean update(ValueType<C> type, Id id, UpdateExpression update, Optional<ConditionExpression> condition, Optional<BaseValue<C>> consumer) throws NotFoundException {
    return false;
  }

  @Override
  public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type) {
    return null;
  }
}
