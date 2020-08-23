package com.dremio.nessie.versioned.impl;

import com.google.protobuf.ByteString;

public class InternalValue extends WrappedValueBean {

  private InternalValue(Id id, ByteString value) {
    super(id, value);
  }

  public static InternalValue of(ByteString value) {
    return new InternalValue(null, value);
  }

  @Override
  protected long getSeed() {
    return 2829568831168137780L;
  }

  public static final SimpleSchema<InternalValue> SCHEMA = new WrappedValueBean.WrappedValueSchema<>(InternalValue.class, InternalValue::new);
}
