/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.versioned.gc;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
import com.dremio.nessie.versioned.store.dynamo.DynamoStore;

/**
 * A store that allows us to override the dt values on save.
 */
class DtAdjustingStore implements Store {

  private Long override;

  private final DynamoStore delegate;

  public DtAdjustingStore(DynamoStore delegate) {
    super();
    this.delegate = delegate;
  }

  public void setOverride(long value) {
    this.override = value;
  }

  public void clearOverride() {
    this.override = null;
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void load(LoadStep loadstep) {
    delegate.load(loadstep);
  }

  public class DtOverwrite implements java.lang.reflect.InvocationHandler {

    private final Object delegate;

    public DtOverwrite(Object delegate) {
      this.delegate = delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (override != null && method.getName().equals("dt")) {
        return method.invoke(delegate, new Object[] {override});
      }
      return method.invoke(delegate, args);
    }

  }

  @Override
  public <C extends BaseValue<C>> boolean putIfAbsent(SaveOp<C> saveOp) {
    return delegate.putIfAbsent(wrap(saveOp));
  }

  private <C extends BaseValue<C>> SaveOp<C> wrap(SaveOp<C> op) {
    Class<C> iface = op.getType().getValueClass();

    return new SaveOp<C>(op.getType(), op.getId()) {
      @SuppressWarnings("unchecked")
      @Override
      public void serialize(C consumer) {
        C newProxy = (C) Proxy.newProxyInstance(
            Thread.currentThread().getContextClassLoader(),
            new Class<?>[] {iface},
            new DtOverwrite(consumer));
        op.serialize(newProxy);
      }
    };
  }

  @Override
  public <C extends BaseValue<C>> void put(SaveOp<C> saveOp, Optional<ConditionExpression> condition) {
    delegate.put(wrap(saveOp), condition);
  }

  @Override
  public <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id, Optional<ConditionExpression> condition) {
    return delegate.delete(type, id, condition);
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    delegate.save(ops.stream().map(this::wrap).collect(Collectors.toList()));
  }

  @Override
  public <C extends BaseValue<C>> void loadSingle(ValueType<C> type, Id id, C consumer) {
    delegate.loadSingle(type, id, consumer);
  }

  @Override
  public <C extends BaseValue<C>> boolean update(ValueType<C> type, Id id, UpdateExpression update,
      Optional<ConditionExpression> condition, Optional<BaseValue<C>> consumer) throws NotFoundException {
    return delegate.update(type, id, update, condition, consumer);
  }

  @Override
  public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type) {
    return delegate.getValues(type);
  }

  public void deleteTables() {
    delegate.deleteTables();
  }

}
