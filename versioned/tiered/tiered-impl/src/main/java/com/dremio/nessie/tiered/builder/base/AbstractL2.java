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

package com.dremio.nessie.tiered.builder.base;

import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L2;
import com.dremio.nessie.versioned.store.Id;

/**
 * Abstract implementation of {@link L2}, all methods return {@code this}.
 */
public abstract class AbstractL2 extends AbstractBaseValue<L2> implements L2 {
  @Override
  public L2 children(Stream<Id> ids) {
    return this;
  }
}
