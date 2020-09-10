package com.dremio.nessie.hms;

import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.dremio.nessie.hms.HMSProto.TableAndPartitions;
import com.dremio.nessie.versioned.Serializer;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnsafeByteOperations;

class ItemSerializer implements Serializer<Item> {

  @Override
  public ByteString toBytes(Item value) {
    HMSProto.Item.Builder builder = HMSProto.Item.newBuilder();
    switch (value.getType()) {
      case CATALOG:
        return builder.setCatalog(toBytes(value.getCatalog())).build().toByteString();
      case DATABASE:
        return builder.setDatabase(toBytes(value.getDatabase())).build().toByteString();
      case TABLE:
        return builder.setTable(
            TableAndPartitions.newBuilder()
            .setTable(toBytes(value.getTable()))
            .addAllPartitions(value.getPartitions()
                .stream()
                .map(ItemSerializer::toBytes)
                .collect(Collectors.toList())))
            .build()
            .toByteString();
      default:
        throw new IllegalStateException("Unknown type of " + value.getType());
    }
  }

  private static ByteString toBytes(TBase<?, ?> base) {
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    try {
      return UnsafeByteOperations.unsafeWrap(serializer.serialize(base));
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T extends TBase<T, ?>> T fromBytes(T empty, ByteString bytes) {
    TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
    try {
      deserializer.deserialize(empty, bytes.toByteArray());
      return empty;
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Item fromBytes(ByteString bytes) {
    try {
      HMSProto.Item item = HMSProto.Item.parseFrom(bytes);
      switch(item.getHiveObjCase()) {
        case CATALOG:
          return Item.wrap(fromBytes(new Catalog(), item.getCatalog()));
        case DATABASE:
          return Item.wrap(fromBytes(new Database(), item.getDatabase()));
        case TABLE:
          return Item.wrap(
              fromBytes(new Table(), item.getTable().getTable()),
              item.getTable().getPartitionsList().stream().map(p -> fromBytes(new Partition(), p)).collect(Collectors.toList())
              );
        default:
        case HIVEOBJ_NOT_SET:
          throw new IllegalStateException("Unable to consume object.");
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

}
