package com.dremio.nessie.hms;

import com.dremio.nessie.hms.HMSProto.CommitMetadata;
import com.dremio.nessie.versioned.Serializer;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class CommitSerializer implements Serializer<CommitMetadata> {

  @Override
  public ByteString toBytes(CommitMetadata value) {
    return value.toByteString();
  }

  @Override
  public CommitMetadata fromBytes(ByteString bytes) {
    try {
      return CommitMetadata.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

}
