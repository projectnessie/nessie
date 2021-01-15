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
package com.dremio.nessie.versioned.store;

import com.dremio.nessie.tiered.builder.BaseConsumer;

/**
 * To handle or map the values retrieved in a batch-load operation like
 * {@link Store#getValues(Class, ValueType, ValuesMapper)}, implementations of this interface
 * provide the functionality to handle or map the loaded values by providing a new
 * {@link BaseConsumer} via {@link #producerForItem()} and receiving the callback to
 * {@link #itemProduced(BaseConsumer)} once all properties have been pushed to that consumer.
 *
 * @param <C> {@link BaseConsumer consumer} used to handle the load-operation
 * @param <V> value type that will be passed through by the store-implementation
 */
public interface ValuesMapper<C extends BaseConsumer<C>, V>  {
  C producerForItem();

  V itemProduced(C producer);
}
