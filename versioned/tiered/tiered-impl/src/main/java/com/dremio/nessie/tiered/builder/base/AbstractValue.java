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

import com.dremio.nessie.tiered.builder.Value;
import com.dremio.nessie.versioned.store.Id;
import com.google.protobuf.ByteString;

/**
 * Abstract implementation of {@link Value}, all methods return {@code this}.
 * <p>All {@code Abstract*} classes in this package are meant to ease consumption of values loaded
 * via {@link com.dremio.nessie.versioned.VersionStore}, so users do not have to implement every
 * method.</p>
 */
public abstract class AbstractValue implements Value {
  @Override
  public Value id(Id id) {
    return this;
  }

  @Override
  public Value dt(long dt) {
    return this;
  }

  @Override
  public Value value(ByteString value) {
    return this;
  }
}
