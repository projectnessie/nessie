package com.dremio.nessie.versioned.impl;

import com.google.protobuf.ByteString;

public class InternalCommitMetadata extends WrappedValueBean {

  private InternalCommitMetadata(Id id, ByteString value) {
    super(id, value);
  }

  public static InternalCommitMetadata of(ByteString value) {
    return new InternalCommitMetadata(null, value);
  }

  @Override
  protected long getSeed() {
    return 2279557414590649190L;
  }

  public static final SimpleSchema<InternalCommitMetadata> SCHEMA = new WrappedValueBean.WrappedValueSchema<>(InternalCommitMetadata.class, InternalCommitMetadata::new);
}
