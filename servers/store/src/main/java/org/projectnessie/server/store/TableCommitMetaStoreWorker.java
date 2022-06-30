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

import static org.projectnessie.model.IcebergContent.CURRENT_SCHEMA_ID;
import static org.projectnessie.model.IcebergContent.CURRENT_SNAPSHOT_ID;
import static org.projectnessie.model.IcebergContent.CURRENT_VERSION_ID;
import static org.projectnessie.model.IcebergContent.DEFAULT_SORT_ORDER_ID;
import static org.projectnessie.model.IcebergContent.DEFAULT_SPEC_ID;
import static org.projectnessie.model.IcebergContent.VERSIONS;
import static org.projectnessie.model.IcebergContent.VERSION_ID;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.GenericMetadata;
import org.projectnessie.model.IcebergContent;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.Namespace;
import org.projectnessie.server.store.proto.ObjectTypes;
import org.projectnessie.server.store.proto.ObjectTypes.ContentPartReference;
import org.projectnessie.server.store.proto.ObjectTypes.ContentPartType;
import org.projectnessie.versioned.ContentAttachment;
import org.projectnessie.versioned.ContentAttachment.Compression;
import org.projectnessie.versioned.ContentAttachment.Format;
import org.projectnessie.versioned.ContentAttachmentKey;
import org.projectnessie.versioned.ImmutableContentAttachment;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.StoreWorker;

public class TableCommitMetaStoreWorker implements StoreWorker<Content, CommitMeta, Content.Type> {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final CBORMapper CBOR_MAPPER = new CBORMapper();

  private static final Format PREFERRED_ATTACHMENT_FORMAT = Format.JSON;

  private static final EnumMap<ContentPartType, String> JSON_ARRAY_NAMES_FOR_PART_TYPE;

  static {
    JSON_ARRAY_NAMES_FOR_PART_TYPE = new EnumMap<>(ContentPartType.class);
    JSON_ARRAY_NAMES_FOR_PART_TYPE.put(
        ContentPartType.PARTITION_SPEC, IcebergContent.PARTITION_SPECS);
    JSON_ARRAY_NAMES_FOR_PART_TYPE.put(ContentPartType.SNAPSHOT, IcebergContent.SNAPSHOTS);
    JSON_ARRAY_NAMES_FOR_PART_TYPE.put(ContentPartType.SCHEMA, IcebergContent.SCHEMAS);
    JSON_ARRAY_NAMES_FOR_PART_TYPE.put(ContentPartType.SORT_ORDER, IcebergContent.SORT_ORDERS);
    JSON_ARRAY_NAMES_FOR_PART_TYPE.put(ContentPartType.VERSION, VERSIONS);
  }

  private final Serializer<CommitMeta> metaSerializer = new MetadataSerializer();

  @Override
  public ByteString toStoreOnReferenceState(
      Content content, Consumer<ContentAttachment> attachmentConsumer) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder().setId(content.getId());
    if (content instanceof IcebergTable) {
      toStoreIcebergTable((IcebergTable) content, attachmentConsumer, builder);
    } else if (content instanceof IcebergView) {
      toStoreIcebergView((IcebergView) content, attachmentConsumer, builder);
    } else if (content instanceof DeltaLakeTable) {
      toStoreDeltaLakeTable((DeltaLakeTable) content, builder);
    } else if (content instanceof Namespace) {
      toStoreNamespace((Namespace) content, builder);
    } else {
      throw new IllegalArgumentException("Unknown type " + content);
    }

    return builder.build().toByteString();
  }

  private static void toStoreDeltaLakeTable(
      DeltaLakeTable content, ObjectTypes.Content.Builder builder) {
    ObjectTypes.DeltaLakeTable.Builder table =
        ObjectTypes.DeltaLakeTable.newBuilder()
            .addAllMetadataLocationHistory(content.getMetadataLocationHistory())
            .addAllCheckpointLocationHistory(content.getCheckpointLocationHistory());
    String lastCheckpoint = content.getLastCheckpoint();
    if (lastCheckpoint != null) {
      table.setLastCheckpoint(lastCheckpoint);
    }
    builder.setDeltaLakeTable(table);
  }

  private static void toStoreNamespace(Namespace content, ObjectTypes.Content.Builder builder) {
    builder.setNamespace(
        ObjectTypes.Namespace.newBuilder()
            .addAllElements(content.getElements())
            .putAllProperties(content.getProperties())
            .build());
  }

  private static void toStoreIcebergView(
      IcebergView view,
      Consumer<ContentAttachment> attachmentConsumer,
      ObjectTypes.Content.Builder builder) {
    ObjectTypes.IcebergViewState.Builder stateBuilder =
        ObjectTypes.IcebergViewState.newBuilder()
            .setVersionId(view.getVersionId())
            .setSchemaId(view.getSchemaId())
            .setDialect(view.getDialect())
            .setSqlText(view.getSqlText())
            .setMetadataLocation(view.getMetadataLocation());

    extractIcebergAttachments(
        attachmentConsumer,
        view.getMetadata(),
        view.getId(),
        formatVersion -> formatVersion == 1,
        stateBuilder::setMetadata,
        stateBuilder::addCurrentParts,
        stateBuilder::addMoreParts);

    builder.setIcebergViewState(stateBuilder);
  }

  private static void toStoreIcebergTable(
      IcebergTable table,
      Consumer<ContentAttachment> attachmentConsumer,
      ObjectTypes.Content.Builder builder) {
    ObjectTypes.IcebergRefState.Builder stateBuilder =
        ObjectTypes.IcebergRefState.newBuilder()
            .setSnapshotId(table.getSnapshotId())
            .setSchemaId(table.getSchemaId())
            .setSpecId(table.getSpecId())
            .setSortOrderId(table.getSortOrderId())
            .setMetadataLocation(table.getMetadataLocation());

    extractIcebergAttachments(
        attachmentConsumer,
        table.getMetadata(),
        table.getId(),
        formatVersion -> formatVersion == 2,
        stateBuilder::setMetadata,
        stateBuilder::addCurrentParts,
        stateBuilder::addMoreParts);

    builder.setIcebergRefState(stateBuilder);
  }

  private static void extractIcebergAttachments(
      Consumer<ContentAttachment> attachmentConsumer,
      GenericMetadata genericMetadata,
      String contentId,
      IntPredicate formatVersionChecker,
      Consumer<ObjectTypes.ContentPartReference.Builder> metadataRefConsumer,
      Consumer<ObjectTypes.ContentPartReference.Builder> currentPartRefConsumer,
      Consumer<ObjectTypes.ContentPartReference.Builder> morePartRefConsumer) {
    if (genericMetadata == null) {
      return;
    }
    JsonNode metadataRaw = genericMetadata.getMetadata().deepCopy();
    Preconditions.checkState(
        metadataRaw instanceof ObjectNode, "Parsed JsonNode must be an object");
    ObjectNode metadata = ((ObjectNode) metadataRaw);

    int formatVersion = metadata.get(IcebergContent.FORMAT_VERSION).asInt(-1);
    Preconditions.checkArgument(
        formatVersionChecker.test(formatVersion),
        "Unsupported Iceberg metadata format version %s",
        formatVersion);

    // "Consume" all "child objects" (like snapshots, schemas, sort-orders, partition-specs),
    // leaving a "shallow" `metadata` object.
    ICEBERG_ATTACHMENT_DEFS.forEach(
        def -> {

          // Removes the "container" (aka the arrays in the Iceberg table/view metadata that hold
          // snapshots, schemas, etc).
          JsonNode container = metadata.remove(def.metadataContainerField);
          if (container == null) {
            return;
          }

          Preconditions.checkArgument(
              container.isArray(),
              "Metadata contains field '%s' but it is not an array",
              def.metadataContainerField);

          long currentId = metadata.get(def.metadataIdRefField).asLong();

          for (JsonNode jsonNode : container) {
            long id = jsonNode.get(def.childIdField).asLong();
            extractedAttachment(
                contentId,
                def.contentPartType,
                id,
                jsonNode,
                attachmentConsumer,
                id == currentId ? currentPartRefConsumer : morePartRefConsumer);
          }
        });

    // TODO Once we only return the relevant child objects (current snapshot, schema, etc), we can
    //  clear TableMetadata.snapshotLog and TableMetadata.previousFiles.
    //  Similar for Iceberg views?

    extractedAttachment(
        contentId,
        ContentPartType.SHALLOW_METADATA,
        null,
        metadata,
        attachmentConsumer,
        metadataRefConsumer);
  }

  private static void extractedAttachment(
      String contentId,
      ContentPartType attachmentType,
      Long objectId,
      JsonNode jsonNode,
      Consumer<ContentAttachment> attachmentConsumer,
      Consumer<ObjectTypes.ContentPartReference.Builder> partRefConsumer) {
    String attachmentId = hashForJson(jsonNode);

    ContentAttachmentKey key =
        ContentAttachmentKey.of(contentId, attachmentType.name(), attachmentId);

    ImmutableContentAttachment.Builder attachment = ContentAttachment.builder().key(key);
    if (objectId != null) {
      attachment.objectId(objectId);
    }
    attachmentConsumer.accept(fromJson(attachment, jsonNode).build());

    ObjectTypes.ContentPartReference.Builder partReference =
        ObjectTypes.ContentPartReference.newBuilder()
            .setType(attachmentType)
            .setAttachmentId(attachmentId);
    if (objectId != null) {
      partReference.setObjectId(objectId);
    }

    partRefConsumer.accept(partReference);
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

  private static final class IcebergAttachmentDefinition {
    final ObjectTypes.ContentPartType contentPartType;
    final String metadataContainerField;
    final String metadataIdRefField;
    final String childIdField;

    IcebergAttachmentDefinition(
        ContentPartType contentPartType,
        String metadataContainerField,
        String metadataIdRefField,
        String childIdField) {
      this.contentPartType = contentPartType;
      this.metadataContainerField = metadataContainerField;
      this.metadataIdRefField = metadataIdRefField;
      this.childIdField = childIdField;
    }
  }

  private static final List<IcebergAttachmentDefinition> ICEBERG_ATTACHMENT_DEFS =
      Arrays.asList(
          new IcebergAttachmentDefinition(
              ObjectTypes.ContentPartType.SNAPSHOT,
              IcebergContent.SNAPSHOTS,
              CURRENT_SNAPSHOT_ID,
              IcebergContent.SNAPSHOT_ID),
          new IcebergAttachmentDefinition(
              ObjectTypes.ContentPartType.SCHEMA,
              IcebergContent.SCHEMAS,
              CURRENT_SCHEMA_ID,
              IcebergContent.SCHEMA_ID),
          new IcebergAttachmentDefinition(
              ObjectTypes.ContentPartType.SORT_ORDER,
              IcebergContent.SORT_ORDERS,
              DEFAULT_SORT_ORDER_ID,
              IcebergContent.ORDER_ID),
          new IcebergAttachmentDefinition(
              ObjectTypes.ContentPartType.PARTITION_SPEC,
              IcebergContent.PARTITION_SPECS,
              DEFAULT_SPEC_ID,
              IcebergContent.SPEC_ID),
          new IcebergAttachmentDefinition(
              ObjectTypes.ContentPartType.VERSION, VERSIONS, CURRENT_VERSION_ID, VERSION_ID));

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
        return valueFromStoreDeltaLakeTable(content);

      case ICEBERG_REF_STATE:
        return valueFromStoreIcebergTable(attachmentsRetriever, content, metadataPointerSupplier);

      case ICEBERG_VIEW_STATE:
        return valueFromStoreIcebergView(attachmentsRetriever, content, metadataPointerSupplier);

      case NAMESPACE:
        return valueFromStoreNamespace(content);

      case OBJECTTYPE_NOT_SET:
      default:
        throw new IllegalArgumentException("Unknown type " + content.getObjectTypeCase());
    }
  }

  private static ImmutableDeltaLakeTable valueFromStoreDeltaLakeTable(ObjectTypes.Content content) {
    ObjectTypes.DeltaLakeTable deltaLakeTable = content.getDeltaLakeTable();
    ImmutableDeltaLakeTable.Builder builder =
        ImmutableDeltaLakeTable.builder()
            .id(content.getId())
            .addAllMetadataLocationHistory(deltaLakeTable.getMetadataLocationHistoryList())
            .addAllCheckpointLocationHistory(deltaLakeTable.getCheckpointLocationHistoryList());
    if (deltaLakeTable.hasLastCheckpoint()) {
      builder.lastCheckpoint(content.getDeltaLakeTable().getLastCheckpoint());
    }
    return builder.build();
  }

  private static ImmutableNamespace valueFromStoreNamespace(ObjectTypes.Content content) {
    ObjectTypes.Namespace namespace = content.getNamespace();
    return ImmutableNamespace.builder()
        .id(content.getId())
        .elements(namespace.getElementsList())
        .putAllProperties(namespace.getPropertiesMap())
        .build();
  }

  private static ImmutableIcebergTable valueFromStoreIcebergTable(
      Function<Stream<ContentAttachmentKey>, Stream<ContentAttachment>> attachmentsRetriever,
      ObjectTypes.Content content,
      Supplier<String> metadataPointerSupplier) {
    ObjectTypes.IcebergRefState table = content.getIcebergRefState();
    String metadataLocation =
        table.hasMetadataLocation() ? table.getMetadataLocation() : metadataPointerSupplier.get();

    ImmutableIcebergTable.Builder tableBuilder =
        IcebergTable.builder()
            .metadataLocation(metadataLocation)
            .snapshotId(table.getSnapshotId())
            .schemaId(table.getSchemaId())
            .specId(table.getSpecId())
            .sortOrderId(table.getSortOrderId())
            .id(content.getId());

    if (!table.hasMetadata()) {
      // Old global-state or on-ref representation.
      return tableBuilder.build();
    }

    Stream<ContentPartReference> contentParts =
        Stream.concat(
            Stream.concat(table.getMorePartsList().stream(), table.getCurrentPartsList().stream()),
            Stream.of(table.getMetadata()));

    ObjectNode metadata = mergeAttachmentsIntoMetadata(attachmentsRetriever, content, contentParts);

    // TODO Once we only return the relevant child objects (current snapshot, schema, etc), we can
    //  clear TableMetadata.snapshotLog and TableMetadata.previousFiles
    //  Similar for Iceberg views?

    return tableBuilder
        .metadata(GenericMetadata.of(IcebergContent.ICEBERG_METADATA, metadata))
        .build();
  }

  private static ObjectNode mergeAttachmentsIntoMetadata(
      Function<Stream<ContentAttachmentKey>, Stream<ContentAttachment>> attachmentsRetriever,
      ObjectTypes.Content content,
      Stream<ContentPartReference> contentParts) {

    Stream<ContentAttachmentKey> attachmentKeys =
        contentParts.map(
            partRef ->
                ContentAttachmentKey.of(
                    content.getId(), partRef.getType().name(), partRef.getAttachmentId()));

    EnumMap<ContentPartType, List<JsonNode>> nodesPerPartType =
        new EnumMap<>(ContentPartType.class);

    try (Stream<ContentAttachment> attachments = attachmentsRetriever.apply(attachmentKeys)) {
      attachments.forEach(
          att -> {
            String type = att.getKey().getAttachmentType();
            ContentPartType partType = ContentPartType.valueOf(type);
            nodesPerPartType
                .computeIfAbsent(partType, x -> new ArrayList<>())
                .add(fromBytesAsJson(att));
          });
    }

    ObjectNode metadata =
        (ObjectNode)
            Preconditions.checkNotNull(
                    nodesPerPartType.get(ContentPartType.SHALLOW_METADATA),
                    "No attachment of type SHALLOW_METADATA for content ID %s",
                    content.getId())
                .get(0);

    JSON_ARRAY_NAMES_FOR_PART_TYPE.forEach(
        (type, jsonName) -> addJsonArray(metadata, jsonName, nodesPerPartType.get(type)));

    return metadata;
  }

  private static ImmutableIcebergView valueFromStoreIcebergView(
      Function<Stream<ContentAttachmentKey>, Stream<ContentAttachment>> attachmentsRetriever,
      ObjectTypes.Content content,
      Supplier<String> metadataPointerSupplier) {
    ObjectTypes.IcebergViewState view = content.getIcebergViewState();
    // If the (protobuf) view has the metadataLocation attribute set, use that one, otherwise
    // it's an old representation using global state.
    String metadataLocation =
        view.hasMetadataLocation() ? view.getMetadataLocation() : metadataPointerSupplier.get();

    ImmutableIcebergView.Builder viewBuilder =
        IcebergView.builder()
            .metadataLocation(metadataLocation)
            .versionId(view.getVersionId())
            .schemaId(view.getSchemaId())
            .dialect(view.getDialect())
            .sqlText(view.getSqlText())
            .id(content.getId());

    if (!view.hasMetadata()) {
      // Old global-state or on-ref representation.
      return viewBuilder.build();
    }

    Stream<ContentPartReference> contentParts =
        Stream.concat(
            Stream.of(view.getMetadata()),
            Stream.concat(view.getCurrentPartsList().stream(), view.getMorePartsList().stream()));

    ObjectNode metadata = mergeAttachmentsIntoMetadata(attachmentsRetriever, content, contentParts);

    return viewBuilder
        .metadata(GenericMetadata.of(IcebergContent.ICEBERG_METADATA, metadata))
        .build();
  }

  private static void addJsonArray(JsonNode jsonNode, String fieldName, List<JsonNode> retrieved) {
    if (retrieved != null) {
      ArrayNode array = jsonNode.withArray(fieldName);
      array.addAll(retrieved);
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  private static String hashForJson(JsonNode jsonNode) {
    Hasher h = Hashing.sha256().newHasher();
    hashForJson(h, jsonNode);
    return h.hash().toString();
  }

  @SuppressWarnings("UnstableApiUsage")
  private static void hashForJson(Hasher h, JsonNode jsonNode) {
    switch (jsonNode.getNodeType()) {
      case ARRAY:
        jsonNode.elements().forEachRemaining(n -> hashForJson(h, n));
        break;
      case OBJECT:
        jsonNode
            .fields()
            .forEachRemaining(
                e -> {
                  h.putUnencodedChars(e.getKey());
                  hashForJson(h, e.getValue());
                });
        break;
      case BINARY:
        try {
          h.putBytes(jsonNode.binaryValue());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        break;
      case BOOLEAN:
        h.putBoolean(jsonNode.booleanValue());
        break;
      case NUMBER:
        h.putLong(jsonNode.longValue());
        break;
      case POJO:
      case STRING:
        h.putUnencodedChars(jsonNode.asText());
        break;
      case MISSING:
      case NULL:
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unexpected node type %s in JSON node %s", jsonNode.getNodeType(), jsonNode));
    }
  }

  private static ImmutableContentAttachment.Builder fromJson(
      ImmutableContentAttachment.Builder attachment, JsonNode jsonNode) {

    attachment.compression(Compression.NONE);

    switch (PREFERRED_ATTACHMENT_FORMAT) {
      case CBOR:
        try (Output out = ByteString.newOutput()) {
          CBOR_MAPPER.writeValue(out, jsonNode);
          ByteString data = out.toByteString();
          return attachment.format(Format.CBOR).data(data);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      case JSON:
      default:
        return attachment.format(Format.JSON).data(ByteString.copyFromUtf8(jsonNode.toString()));
    }
  }

  private static ObjectNode fromBytesAsJson(ContentAttachment attachment) {
    try {
      switch (attachment.getFormat()) {
        case JSON:
          return (ObjectNode) MAPPER.readValue(attachment.getData().toStringUtf8(), JsonNode.class);
        case CBOR:
          try (JsonParser parser = CBOR_MAPPER.createParser(attachment.getData().newInput())) {
            return (ObjectNode) parser.readValueAs(JsonNode.class);
          }
        default:
          throw new IllegalStateException("Compression not supported");
      }
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to deserialize attachment '%s'", attachment.getKey()), e);
    }
  }

  private static IllegalArgumentException noIcebergMetadataPointer() {
    return new IllegalArgumentException(
        "Iceberg content from reference must have global state, but has none");
  }

  @Override
  public Content applyId(Content content, String id) {
    Objects.requireNonNull(content, "content must not be null");
    Preconditions.checkArgument(content.getId() == null, "content.getId() must be null");
    Objects.requireNonNull(id, "id must not be null");
    if (content instanceof IcebergTable) {
      return IcebergTable.builder().from(content).id(id).build();
    } else if (content instanceof DeltaLakeTable) {
      return ImmutableDeltaLakeTable.builder().from(content).id(id).build();
    } else if (content instanceof IcebergView) {
      return IcebergView.builder().from(content).id(id).build();
    } else if (content instanceof Namespace) {
      return ImmutableNamespace.builder().from(content).id(id).build();
    } else {
      throw new IllegalArgumentException("Unknown type " + content);
    }
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
  public Content.Type getType(Content content) {
    return content.getType();
  }

  @Override
  public Content.Type getType(Byte payload) {
    if (payload == null || payload > Content.Type.values().length || payload < 0) {
      throw new IllegalArgumentException(
          String.format("Cannot create type from payload. Payload %d does not exist", payload));
    }
    return Content.Type.values()[payload];
  }

  @Override
  public Content.Type getType(ByteString onRefContent) {
    ObjectTypes.Content parsed = parse(onRefContent);

    if (parsed.hasIcebergRefState()) {
      return Content.Type.ICEBERG_TABLE;
    }
    if (parsed.hasIcebergViewState()) {
      return Content.Type.ICEBERG_VIEW;
    }

    if (parsed.hasDeltaLakeTable()) {
      return Content.Type.DELTA_LAKE_TABLE;
    }

    if (parsed.hasNamespace()) {
      return Content.Type.NAMESPACE;
    }

    throw new IllegalArgumentException("Unsupported on-ref content " + parsed);
  }

  @Override
  public boolean requiresGlobalState(Content content) {
    switch (content.getType()) {
      case ICEBERG_TABLE:
        // yes, Iceberg Tables used global state before, but no longer do so
      case ICEBERG_VIEW:
        // yes, Iceberg Views used global state before, but no longer do so
      case DELTA_LAKE_TABLE:
      case NAMESPACE:
      default:
        return false;
    }
  }

  @Override
  public boolean requiresGlobalState(ByteString content) {
    ObjectTypes.Content parsed = parse(content);
    switch (parsed.getObjectTypeCase()) {
      case ICEBERG_REF_STATE:
        return !parsed.getIcebergRefState().hasMetadataLocation();
      case ICEBERG_VIEW_STATE:
        return !parsed.getIcebergViewState().hasMetadataLocation();
      default:
        return false;
    }
  }

  private static ObjectTypes.Content parse(ByteString value) {
    try {
      return ObjectTypes.Content.parseFrom(value);
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
      return Content.Type.NAMESPACE == getType(type);
    } catch (Exception e) {
      return false;
    }
  }
}
