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
package com.dremio.nessie.versioned.impl;

import com.google.protobuf.ByteString;

/**
 * Holds a VersionStore binary value for interaction with the Store
 */
public class InternalValue extends WrappedValueBean {

  private InternalValue(Id id, ByteString value) {
    super(id, value);
  }

  public static InternalValue of(ByteString value) {
    return new InternalValue(null, value);
  }

  @Override
  protected long getSeed() {
    return 2829568831168137780L; // an arbitrary but consistent seed to ensure no hash conflicts.
  }

  public static final SimpleSchema<InternalValue> SCHEMA = new WrappedValueBean.WrappedValueSchema<>(InternalValue.class, InternalValue::new);
}
