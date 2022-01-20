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
package org.projectnessie.server.store;

import static org.projectnessie.model.IcebergTable.DEFAULT_SNAPSHOT_ID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableDeltaLakeTable.Builder;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.iceberg.IcebergPartitionSpec;
import org.projectnessie.model.iceberg.IcebergSchema;
import org.projectnessie.model.iceberg.IcebergSnapshot;
import org.projectnessie.model.iceberg.IcebergSortOrder;
import org.projectnessie.model.iceberg.IcebergViewDefinition;
import org.projectnessie.model.iceberg.IcebergViewVersion;
import org.projectnessie.model.iceberg.ImmutableIcebergViewDefinition;
import org.projectnessie.server.store.proto.ObjectTypes;
import org.projectnessie.server.store.proto.ObjectTypes.IcebergPerContentType;
import org.projectnessie.server.store.proto.ObjectTypes.IcebergRefState;
import org.projectnessie.server.store.proto.ObjectTypes.IcebergTablePerContentState;
import org.projectnessie.server.store.proto.ObjectTypes.IcebergViewPerContentState;
import org.projectnessie.server.store.proto.ObjectTypes.IcebergViewState;
import org.projectnessie.versioned.ContentAttachment;
import org.projectnessie.versioned.ContentAttachment.Compression;
import org.projectnessie.versioned.ContentAttachmentKey;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.StoreWorker;

public class TableCommitMetaStoreWorker implements StoreWorker<Content, CommitMeta, Content.Type> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Serializer<CommitMeta> metaSerializer = new MetadataSerializer();

  @Override
  public ByteString toStoreOnReferenceState(Content content) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder().setId(content.getId());
    if (content instanceof IcebergTable) {
      IcebergTable table = (IcebergTable) content;
      ObjectTypes.IcebergRefState.Builder stateBuilder =
          ObjectTypes.IcebergRefState.newBuilder()
              .setSnapshotId(table.getSnapshotId())
              .setSchemaId(table.getSchemaId())
              .setSpecId(table.getSpecId())
              .setSortOrderId(table.getSortOrderId());
      if (table.getUuid() != null) {
        stateBuilder.setUuid(table.getUuid());
      }
      builder.setIcebergRefState(stateBuilder);
    } else if (content instanceof IcebergView) {
      IcebergView view = (IcebergView) content;
      builder.setIcebergViewState(
          ObjectTypes.IcebergViewState.newBuilder()
              .setVersionId(view.getVersionId())
              .setSchemaId(view.getSchemaId())
              .setDialect(view.getDialect())
              .setSqlText(view.getSqlText()));
    } else if (content instanceof DeltaLakeTable) {
      ObjectTypes.DeltaLakeTable.Builder table =
          ObjectTypes.DeltaLakeTable.newBuilder()
              .addAllMetadataLocationHistory(
                  ((DeltaLakeTable) content).getMetadataLocationHistory())
              .addAllCheckpointLocationHistory(
                  ((DeltaLakeTable) content).getCheckpointLocationHistory());
      String lastCheckpoint = ((DeltaLakeTable) content).getLastCheckpoint();
      if (lastCheckpoint != null) {
        table.setLastCheckpoint(lastCheckpoint);
      }
      builder.setDeltaLakeTable(table);
    } else if (content instanceof Namespace) {
      Namespace ns = (Namespace) content;
      builder.setNamespace(ObjectTypes.Namespace.newBuilder().addAllElements(ns.getElements()));
    } else {
      throw new IllegalArgumentException("Unknown type " + content);
    }

    return builder.build().toByteString();
  }

  @Override
  public ByteString toStoreGlobalState(Content content) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder().setId(content.getId());
    if (content instanceof IcebergTable) {
      IcebergTable state = (IcebergTable) content;
      ObjectTypes.IcebergMetadataPointer.Builder stateBuilder =
          ObjectTypes.IcebergMetadataPointer.newBuilder()
              .setMetadataLocation(state.getMetadataLocation());
      builder.setIcebergMetadataPointer(stateBuilder);
    } else if (content instanceof IcebergView) {
      IcebergView state = (IcebergView) content;
      ObjectTypes.IcebergMetadataPointer.Builder stateBuilder =
          ObjectTypes.IcebergMetadataPointer.newBuilder()
              .setMetadataLocation(state.getMetadataLocation());
      builder.setIcebergMetadataPointer(stateBuilder);
    } else {
      throw new IllegalArgumentException("Unknown type " + content);
    }

    return builder.build().toByteString();
  }

  @Override
  public void toStoreAttachments(Content content, Consumer<ContentAttachment> attachmentConsumer) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder().setId(content.getId());
    if (content instanceof IcebergTable) {
      IcebergTable state = (IcebergTable) content;
      ObjectTypes.IcebergTablePerContentState.Builder stateBuilder =
          ObjectTypes.IcebergTablePerContentState.newBuilder()
              .setUuid(state.getUuid())
              .setLastColumnId(state.getLastColumnId())
              .setLastAssignedPartitionId(state.getLastAssignedPartitionId())
              .setLastSequenceNumber(state.getLastSequenceNumber());
      builder.setIcebergTablePerContent(stateBuilder);
      attachmentConsumer.accept(
          ContentAttachment.builder()
              .key(
                  ContentAttachmentKey.of(
                      content.getId(),
                      IcebergPerContentType.MAIN.name(),
                      IcebergPerContentType.MAIN.name()))
              .compression(Compression.NONE)
              .data(builder.build().toByteString())
              .build());

      addPerContentStates(
          content.getId(),
          attachmentConsumer,
          state.getSnapshots(),
          IcebergPerContentType.SNAPSHOT,
          IcebergSnapshot::getSnapshotId);
      addPerContentStates(
          content.getId(),
          attachmentConsumer,
          state.getSchemas(),
          IcebergPerContentType.SCHEMA,
          IcebergSchema::getSchemaId);
      addPerContentStates(
          content.getId(),
          attachmentConsumer,
          state.getSpecs(),
          IcebergPerContentType.PARTITION_SPEC,
          IcebergPartitionSpec::getSpecId);
      addPerContentStates(
          content.getId(),
          attachmentConsumer,
          state.getSortOrders(),
          IcebergPerContentType.SCHEMA,
          IcebergSortOrder::getOrderId);
    } else if (content instanceof IcebergView) {
      IcebergView state = (IcebergView) content;
      ObjectTypes.IcebergViewPerContentState.Builder stateBuilder =
          ObjectTypes.IcebergViewPerContentState.newBuilder();
      builder.setIcebergViewPerContent(stateBuilder);
      attachmentConsumer.accept(
          ContentAttachment.builder()
              .key(
                  ContentAttachmentKey.of(
                      content.getId(),
                      IcebergPerContentType.MAIN.name(),
                      IcebergPerContentType.MAIN.name()))
              .compression(Compression.NONE)
              .data(builder.build().toByteString())
              .build());

      if (state.getViewDefinition() != null) {
        addPerContentStates(
            content.getId(),
            attachmentConsumer,
            Collections.singletonList(state.getViewDefinition().getSchema()),
            IcebergPerContentType.SCHEMA,
            IcebergSchema::getSchemaId);
      }
    } else {
      throw new IllegalArgumentException("Unknown type " + content);
    }
  }

  private <T> void addPerContentStates(
      String contentId,
      Consumer<ContentAttachment> attachmentConsumer,
      Collection<T> objects,
      IcebergPerContentType perContentType,
      ToLongFunction<T> idExtractor) {
    for (T object : objects) {
      attachmentConsumer.accept(
          ContentAttachment.builder()
              .key(
                  ContentAttachmentKey.of(
                      contentId,
                      perContentType.name(),
                      Long.toString(idExtractor.applyAsLong(object))))
              .compression(Compression.NONE)
              .data(toJsonAsBytes(object))
              .build());
    }
  }

  private static <T> T fromBytesAsJson(ContentAttachment attachment, Class<T> type) {
    try {
      Class<?> typeClass;
      switch (IcebergPerContentType.valueOf(attachment.getKey().getObjectType())) {
        case SNAPSHOT:
          typeClass = IcebergSnapshot.class;
          break;
        case SCHEMA:
          typeClass = IcebergSchema.class;
          break;
        case PARTITION_SPEC:
          typeClass = IcebergPartitionSpec.class;
          break;
        case SORT_ORDER:
          typeClass = IcebergSortOrder.class;
          break;
        case VERSION:
          typeClass = IcebergViewVersion.class;
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Unknown type '%s'", attachment.getKey().getObjectType()));
      }
      if (typeClass != type) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to deserialize attachment '%s' as '%s', should be '%s'",
                attachment.getKey(), type.getName(), typeClass.getName()));
      }
      T r = (T) MAPPER.readValue(uncompress(attachment).toStringUtf8(), type);
      return r;
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format(
              "Failed to deserialize attachment '%s' as '%s'", attachment.getKey(), type.getName()),
          e);
    }
  }

  private static <T> ByteString toJsonAsBytes(T object) {
    try {
      return ByteString.copyFromUtf8(MAPPER.writeValueAsString(object));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Content valueFromStore(
      ByteString onReferenceValue,
      Supplier<ByteString> globalState,
      Function<Stream<ContentAttachmentKey>, Stream<ContentAttachment>> attachmentsRetriever) {
    ObjectTypes.Content content = parse(onReferenceValue);
    Supplier<String> metadataPointerSupplier =
        () -> {
          ByteString global = globalState.get();
          if (global == null) {
            throw noIcebergMetadataPointer();
          }
          ObjectTypes.Content globalContent = parse(global);
          if (!globalContent.hasIcebergMetadataPointer()) {
            throw noIcebergMetadataPointer();
          }
          return globalContent.getIcebergMetadataPointer().getMetadataLocation();
        };
    switch (content.getObjectTypeCase()) {
      case DELTA_LAKE_TABLE:
        Builder builder =
            ImmutableDeltaLakeTable.builder()
                .id(content.getId())
                .addAllMetadataLocationHistory(
                    content.getDeltaLakeTable().getMetadataLocationHistoryList())
                .addAllCheckpointLocationHistory(
                    content.getDeltaLakeTable().getCheckpointLocationHistoryList());
        if (content.getDeltaLakeTable().getLastCheckpoint() != null) {
          builder.lastCheckpoint(content.getDeltaLakeTable().getLastCheckpoint());
        }
        return builder.build();

      case ICEBERG_REF_STATE:
        IcebergRefState table = content.getIcebergRefState();
        String metadataLocation =
            table.hasMetadataLocation()
                ? table.getMetadataLocation()
                : metadataPointerSupplier.get();

        ImmutableIcebergTable.Builder tableBuilder =
            IcebergTable.builder()
                .metadataLocation(metadataLocation)
                .snapshotId(table.getSnapshotId())
                .schemaId(table.getSchemaId())
                .specId(table.getSpecId())
                .sortOrderId(table.getSortOrderId())
                .id(content.getId());

        if (!table.hasUuid()) {
          // Old global-state or on-ref representation.
          return tableBuilder.build();
        }

        tableBuilder
            .formatVersion(IcebergTable.MAX_SUPPORTED_FORMAT_VERSION)
            .lastUpdatedMillis(table.getLastUpdatedMillis())
            .putAllProperties(table.getPropertiesMap())
            .location(String.format("nessie://%s", content.getId()));

        Stream<ContentAttachmentKey> attachmentKeys =
            Stream.of(
                ContentAttachmentKey.of(
                    content.getId(),
                    IcebergPerContentType.MAIN.name(),
                    IcebergPerContentType.MAIN.name()),
                ContentAttachmentKey.of(
                    content.getId(),
                    IcebergPerContentType.SORT_ORDER.name(),
                    Long.toString(table.getSortOrderId())),
                ContentAttachmentKey.of(
                    content.getId(),
                    IcebergPerContentType.PARTITION_SPEC.name(),
                    Long.toString(table.getSpecId())),
                ContentAttachmentKey.of(
                    content.getId(),
                    IcebergPerContentType.SCHEMA.name(),
                    Long.toString(table.getSchemaId())));
        if (table.getSnapshotId() != DEFAULT_SNAPSHOT_ID) {
          attachmentKeys =
              Stream.concat(
                  attachmentKeys,
                  Stream.of(
                      ContentAttachmentKey.of(
                          content.getId(),
                          IcebergPerContentType.SNAPSHOT.name(),
                          Long.toString(table.getSnapshotId()))));
        }

        Stream<ContentAttachment> attachments = attachmentsRetriever.apply(attachmentKeys);
        attachments.forEach(
            att -> {
              String type = att.getKey().getObjectType();
              switch (IcebergPerContentType.valueOf(type)) {
                case PARTITION_SPEC:
                  tableBuilder.addSpecs(fromBytesAsJson(att, IcebergPartitionSpec.class));
                  break;
                case SNAPSHOT:
                  tableBuilder.addSnapshots(fromBytesAsJson(att, IcebergSnapshot.class));
                  break;
                case SCHEMA:
                  tableBuilder.addSchemas(fromBytesAsJson(att, IcebergSchema.class));
                  break;
                case SORT_ORDER:
                  tableBuilder.addSortOrders(fromBytesAsJson(att, IcebergSortOrder.class));
                  break;
                case MAIN:
                  try {
                    IcebergTablePerContentState main =
                        IcebergTablePerContentState.parseFrom(uncompress(att));
                    tableBuilder
                        .lastAssignedPartitionId(main.getLastAssignedPartitionId())
                        .lastColumnId(main.getLastColumnId())
                        .lastSequenceNumber(main.getLastSequenceNumber());
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                  break;
                default:
                  throw new IllegalStateException("Unknown/unexpected per-content type " + type);
              }
            });

        return tableBuilder.build();

      case ICEBERG_VIEW_STATE:
        ObjectTypes.IcebergViewState view = content.getIcebergViewState();
        // If the (protobuf) view has the metadataLocation attribute set, use that one, otherwise
        // it's an old representation using global state.
        metadataLocation =
            view.hasMetadataLocation()
                ? (view.hasUuid()
                    ? String.format("nessie://%s", content.getId())
                    : view.getMetadataLocation())
                : metadataPointerSupplier.get();

        ImmutableIcebergView.Builder viewBuilder =
            IcebergView.builder()
                .metadataLocation(metadataLocation)
                .versionId(view.getVersionId())
                .schemaId(view.getSchemaId())
                .dialect(view.getDialect())
                .sqlText(view.getSqlText())
                .id(content.getId());

        if (!view.hasUuid()) {
          // Old global-state or on-ref representation.
          return viewBuilder.build();
        }

        viewBuilder
            .formatVersion(IcebergView.MAX_SUPPORTED_FORMAT_VERSION)
            .putAllProperties(view.getPropertiesMap());

        ImmutableIcebergViewDefinition.Builder viewDefinitionBuilder =
            IcebergViewDefinition.builder()
                .sessionNamespace(view.getSessionNamespaceList())
                .sessionCatalog(view.getSessionCatalog())
                .sql(view.getSqlText());

        attachmentKeys =
            Stream.of(
                ContentAttachmentKey.of(
                    content.getId(),
                    IcebergPerContentType.MAIN.name(),
                    IcebergPerContentType.MAIN.name()),
                ContentAttachmentKey.of(
                    content.getId(),
                    IcebergPerContentType.VERSION.name(),
                    Long.toString(view.getVersionId())),
                ContentAttachmentKey.of(
                    content.getId(),
                    IcebergPerContentType.SCHEMA.name(),
                    Long.toString(view.getSchemaId())));

        attachments = attachmentsRetriever.apply(attachmentKeys);
        attachments.forEach(
            att -> {
              String type = att.getKey().getObjectType();
              switch (IcebergPerContentType.valueOf(type)) {
                case VERSION:
                  viewBuilder.addVersions(fromBytesAsJson(att, IcebergViewVersion.class));
                  break;
                case SCHEMA:
                  viewDefinitionBuilder.schema(fromBytesAsJson(att, IcebergSchema.class));
                  break;
                case MAIN:
                  try {
                    IcebergViewPerContentState main =
                        IcebergViewPerContentState.parseFrom(uncompress(att));
                    // TODO nothing there?
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                  break;
                default:
                  throw new IllegalStateException("Unknown/unexpected per-content type " + type);
              }
            });

        // Only apply the view-definition, when the schema's present for backwards compatibility.
        ImmutableIcebergViewDefinition viewDefinition = viewDefinitionBuilder.build();
        if (viewDefinition.getSchema() != null) {
          viewBuilder.viewDefinition(viewDefinitionBuilder.build());
        }

        return viewBuilder.build();

      case NAMESPACE:
        ObjectTypes.Namespace namespace = content.getNamespace();
        return ImmutableNamespace.builder().elements(namespace.getElementsList()).build();

      case OBJECTTYPE_NOT_SET:
      default:
        throw new IllegalArgumentException("Unknown type " + content.getObjectTypeCase());
    }
  }

  private static ByteString uncompress(ContentAttachment att) {
    switch (att.getCompression()) {
      case NONE:
        return att.getData();
      default:
        throw new IllegalStateException("Compression not supported");
    }
  }

  private static IllegalArgumentException noIcebergMetadataPointer() {
    return new IllegalArgumentException(
        "Iceberg content from reference must have global state, but has none");
  }

  @Override
  public String getId(Content content) {
    return content.getId();
  }

  @Override
  public Byte getPayload(Content content) {
    if (content instanceof IcebergTable) {
      return (byte) Content.Type.ICEBERG_TABLE.ordinal();
    } else if (content instanceof DeltaLakeTable) {
      return (byte) Content.Type.DELTA_LAKE_TABLE.ordinal();
    } else if (content instanceof IcebergView) {
      return (byte) Content.Type.ICEBERG_VIEW.ordinal();
    } else if (content instanceof Namespace) {
      return (byte) Content.Type.NAMESPACE.ordinal();
    } else {
      throw new IllegalArgumentException("Unknown type " + content);
    }
  }

  @Override
  public Type getType(Content content) {
    if (content instanceof IcebergTable) {
      return Type.ICEBERG_TABLE;
    } else if (content instanceof IcebergView) {
      return Type.ICEBERG_VIEW;
    } else if (content instanceof DeltaLakeTable) {
      return Type.DELTA_LAKE_TABLE;
    } else if (content instanceof Namespace) {
      return Type.NAMESPACE;
    }
    throw new IllegalArgumentException("Unknown type " + content);
  }

  @Override
  public Type getType(Byte payload) {
    if (payload == null || payload > Content.Type.values().length || payload < 0) {
      throw new IllegalArgumentException(
          String.format("Cannot create type from payload. Payload %d does not exist", payload));
    }
    return Content.Type.values()[payload];
  }

  @Override
  public Type getType(ByteString onRefContent) {
    ObjectTypes.Content parsed = parse(onRefContent);

    if (parsed.hasIcebergViewState()) {
      return Type.ICEBERG_TABLE;
    }
    if (parsed.hasIcebergRefState()) {
      return Type.ICEBERG_VIEW;
    }

    if (parsed.hasDeltaLakeTable()) {
      return Type.DELTA_LAKE_TABLE;
    }

    if (parsed.hasNamespace()) {
      return Type.NAMESPACE;
    }

    throw new IllegalArgumentException("Unsupported on-ref content " + parsed);
  }

  @Override
  public boolean requiresGlobalState(Content content) {
    if (content instanceof IcebergTable) {
      IcebergTable table = (IcebergTable) content;
      return table.getUuid() == null;
    }
    if (content instanceof IcebergView) {
      IcebergView view = (IcebergView) content;
      return view.getViewDefinition() == null;
    }
    return false;
  }

  @Override
  public boolean requiresGlobalState(ByteString content) {
    ObjectTypes.Content parsed = parse(content);
    if (parsed.hasIcebergRefState()) {
      IcebergRefState table = parsed.getIcebergRefState();
      return !table.hasUuid();
    }
    if (parsed.hasIcebergViewState()) {
      IcebergViewState view = parsed.getIcebergViewState();
      return !view.hasUuid();
    }
    return false;
  }

  @Override
  public boolean requiresGlobalState(Enum<Type> type) {
    return type == Type.ICEBERG_TABLE || type == Type.ICEBERG_VIEW;
  }

  @Override
  public boolean requiresPerContentState(Content content) {
    if (content instanceof IcebergTable) {
      IcebergTable table = (IcebergTable) content;
      return table.getUuid() != null;
    }
    if (content instanceof IcebergView) {
      IcebergView view = (IcebergView) content;
      return view.getViewDefinition() != null;
    }
    return false;
  }

  @Override
  public boolean requiresPerContentState(ByteString content) {
    ObjectTypes.Content parsed = parse(content);
    if (parsed.hasIcebergRefState()) {
      IcebergRefState table = parsed.getIcebergRefState();
      return table.hasUuid();
    }
    if (parsed.hasIcebergViewState()) {
      IcebergViewState view = parsed.getIcebergViewState();
      return view.hasUuid();
    }
    return false;
  }

  @Override
  public boolean requiresPerContentState(Enum<Type> type) {
    return type == Type.ICEBERG_TABLE || type == Type.ICEBERG_VIEW;
  }

  private static ObjectTypes.Content parse(ByteString onReferenceValue) {
    try {
      return ObjectTypes.Content.parseFrom(onReferenceValue);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failure parsing data", e);
    }
  }

  @Override
  public Serializer<CommitMeta> getMetadataSerializer() {
    return metaSerializer;
  }

  private static class MetadataSerializer implements Serializer<CommitMeta> {
    @Override
    public ByteString toBytes(CommitMeta value) {
      try {
        return ByteString.copyFrom(MAPPER.writeValueAsBytes(value));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(String.format("Couldn't serialize commit meta %s", value), e);
      }
    }

    @Override
    public CommitMeta fromBytes(ByteString bytes) {
      try {
        return MAPPER.readValue(bytes.toByteArray(), CommitMeta.class);
      } catch (IOException e) {
        return ImmutableCommitMeta.builder()
            .message("unknown")
            .committer("unknown")
            .hash("unknown")
            .build();
      }
    }
  }

  @Override
  public boolean isNamespace(ByteString type) {
    try {
      return Type.NAMESPACE == getType(type);
    } catch (Exception e) {
      return false;
    }
  }
}
