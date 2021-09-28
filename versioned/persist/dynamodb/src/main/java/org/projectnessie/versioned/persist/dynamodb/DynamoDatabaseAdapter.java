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
package org.projectnessie.versioned.persist.dynamodb;

import static org.projectnessie.versioned.persist.dynamodb.Tables.KEY_NAME;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_COMMIT_LOG;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_GLOBAL_LOG;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_GLOBAL_POINTER;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_KEY_LISTS;
import static org.projectnessie.versioned.persist.dynamodb.Tables.VALUE_NAME;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.toProto;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.versioned.BackendLimitExceededException;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyWithType;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.ProtoSerialization;
import org.projectnessie.versioned.persist.serialize.ProtoSerialization.Parser;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.LimitExceededException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoDatabaseAdapter
    extends NonTransactionalDatabaseAdapter<NonTransactionalDatabaseAdapterConfig> {

  // DynamoDB limit
  private static final int DYNAMO_BATCH_WRITE_MAX_REQUESTS = 25;

  private static final char PREFIX_SEPARATOR = ':';
  private final DynamoDatabaseClient client;

  private final String keyPrefix;
  private final Map<String, AttributeValue> globalPointerKeyMap;

  public DynamoDatabaseAdapter(
      NonTransactionalDatabaseAdapterConfig config, DynamoDatabaseClient c) {
    super(config);

    Objects.requireNonNull(
        c, "Requires a non-null DynamoDatabaseClient from DynamoDatabaseAdapterConfig");

    client = c;

    String keyPrefix = config.getKeyPrefix();
    if (keyPrefix.indexOf(PREFIX_SEPARATOR) >= 0) {
      throw new IllegalArgumentException("Invalid key prefix: " + keyPrefix);
    }
    this.keyPrefix = keyPrefix + PREFIX_SEPARATOR;

    globalPointerKeyMap =
        Collections.singletonMap(KEY_NAME, AttributeValue.builder().s(this.keyPrefix).build());
  }

  // TODO find a way to wrap those DynamoDB-exceptions everywhere - even when returning Stream's
  @Nonnull
  @VisibleForTesting
  static RuntimeException unhandledException(String operation, RuntimeException e) {
    if (e instanceof RequestLimitExceededException) {
      return new BackendLimitExceededException(
          String.format("Dynamo request-limit exceeded during %s.", operation), e);
    } else if (e instanceof LimitExceededException) {
      return new BackendLimitExceededException(
          String.format("Dynamo limit exceeded during %s.", operation), e);
    } else if (e instanceof ProvisionedThroughputExceededException) {
      return new BackendLimitExceededException(
          String.format("Dynamo provisioned throughput exceeded during %s.", operation), e);
    } else {
      return e;
    }
  }

  @Override
  public void reinitializeRepo(String defaultBranchName) {
    client.client.deleteItem(
        b -> b.tableName(TABLE_GLOBAL_POINTER).key(globalPointerKeyMap).build());

    Stream.of(TABLE_GLOBAL_LOG, TABLE_GLOBAL_LOG, TABLE_KEY_LISTS)
        .forEach(
            table ->
                client
                    .client
                    .scanPaginator(b -> b.tableName(table))
                    .forEach(
                        r ->
                            r.items().stream()
                                .map(attrs -> attrs.get(KEY_NAME))
                                .filter(key -> key.s().startsWith(keyPrefix))
                                .forEach(
                                    key ->
                                        client.client.deleteItem(
                                            b ->
                                                b.tableName(table)
                                                    .key(
                                                        Collections.singletonMap(
                                                            KEY_NAME, key))))));

    super.initializeRepo(defaultBranchName);
  }

  private <T> T loadById(String table, Hash id, Parser<T> parser) {
    return loadById(table, id.asString(), parser);
  }

  private <T> T loadById(String table, String id, Parser<T> parser) {
    byte[] data = loadById(table, id);
    try {
      return data != null ? parser.parse(data) : null;
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  private byte[] loadById(String table, String id) {
    Map<String, AttributeValue> key =
        Collections.singletonMap(KEY_NAME, AttributeValue.builder().s(keyPrefix + id).build());

    GetItemResponse response = client.client.getItem(b -> b.tableName(table).key(key));
    if (!response.hasItem()) {
      return null;
    }

    Map<String, AttributeValue> attributes = response.item();
    SdkBytes bytes = attributes.get(VALUE_NAME).b();
    return bytes.asByteArray();
  }

  @Override
  protected GlobalStatePointer fetchGlobalPointer(NonTransactionalOperationContext ctx) {
    return loadById(TABLE_GLOBAL_POINTER, "", GlobalStatePointer::parseFrom);
  }

  @Override
  protected CommitLogEntry fetchFromCommitLog(NonTransactionalOperationContext ctx, Hash hash) {
    return loadById(TABLE_COMMIT_LOG, hash, ProtoSerialization::protoToCommitLogEntry);
  }

  @Override
  protected GlobalStateLogEntry fetchFromGlobalLog(NonTransactionalOperationContext ctx, Hash id) {
    return loadById(TABLE_GLOBAL_LOG, id, GlobalStateLogEntry::parseFrom);
  }

  @Override
  protected List<CommitLogEntry> fetchPageFromCommitLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPageResult(TABLE_COMMIT_LOG, hashes, ProtoSerialization::protoToCommitLogEntry);
  }

  @Override
  protected List<GlobalStateLogEntry> fetchPageFromGlobalLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPageResult(TABLE_GLOBAL_LOG, hashes, GlobalStateLogEntry::parseFrom);
  }

  @Override
  protected Stream<KeyListEntity> fetchKeyLists(
      NonTransactionalOperationContext ctx, List<Hash> keyListsIds) {
    Map<Hash, KeyList> map =
        fetchPage(TABLE_KEY_LISTS, keyListsIds, ProtoSerialization::protoToKeyList);
    return keyListsIds.stream()
        .map(h -> map.containsKey(h) ? KeyListEntity.of(h, map.get(h)) : null);
  }

  @Override
  protected void writeGlobalCommit(
      NonTransactionalOperationContext ctx, GlobalStateLogEntry entry) {
    insert(TABLE_GLOBAL_LOG, Hash.of(entry.getId()).asString(), entry.toByteArray());
  }

  @Override
  protected void writeIndividualCommit(NonTransactionalOperationContext ctx, CommitLogEntry entry) {
    insert(TABLE_COMMIT_LOG, entry.getHash().asString(), toProto(entry).toByteArray());
  }

  @Override
  protected void unsafeWriteGlobalPointer(
      NonTransactionalOperationContext ctx, GlobalStatePointer pointer) {
    insert(TABLE_GLOBAL_POINTER, "", pointer.toByteArray());
  }

  @Override
  protected void writeMultipleCommits(
      NonTransactionalOperationContext ctx, List<CommitLogEntry> entries) {
    batchWrite(
        TABLE_COMMIT_LOG,
        entries,
        CommitLogEntry::getHash,
        e -> ProtoSerialization.toProto(e).toByteArray());
  }

  @Override
  protected void writeKeyListEntities(
      NonTransactionalOperationContext ctx, List<KeyListEntity> newKeyListEntities) {
    batchWrite(
        TABLE_KEY_LISTS,
        newKeyListEntities,
        KeyListEntity::getId,
        e -> ProtoSerialization.toProto(e.getKeys()).toByteArray());
  }

  @Override
  protected boolean globalPointerCas(
      NonTransactionalOperationContext ctx,
      GlobalStatePointer expected,
      GlobalStatePointer newPointer) {
    AttributeValue expectedBytes =
        AttributeValue.builder().b(SdkBytes.fromByteArray(expected.toByteArray())).build();
    AttributeValue newPointerBytes =
        AttributeValue.builder().b(SdkBytes.fromByteArray(newPointer.toByteArray())).build();
    try {
      client.client.updateItem(
          b ->
              b.tableName(TABLE_GLOBAL_POINTER)
                  .key(globalPointerKeyMap)
                  .expected(
                      Collections.singletonMap(
                          VALUE_NAME,
                          ExpectedAttributeValue.builder().value(expectedBytes).build()))
                  .attributeUpdates(
                      Collections.singletonMap(
                          VALUE_NAME,
                          AttributeValueUpdate.builder()
                              .action(AttributeAction.PUT)
                              .value(newPointerBytes)
                              .build())));
      return true;
    } catch (ConditionalCheckFailedException e) {
      return false;
    }
  }

  @Override
  protected void cleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Hash globalId,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists) {
    Map<String, List<WriteRequest>> requestItems = new HashMap<>();
    if (!branchCommits.isEmpty()) {
      requestItems.put(TABLE_COMMIT_LOG, cleanupDeletes(branchCommits));
    }
    if (!newKeyLists.isEmpty()) {
      requestItems.put(TABLE_KEY_LISTS, cleanupDeletes(newKeyLists));
    }
    requestItems.put(TABLE_GLOBAL_LOG, cleanupDeletes(Collections.singleton(globalId)));

    client.client.batchWriteItem(b -> b.requestItems(requestItems));
  }

  private List<WriteRequest> cleanupDeletes(Set<Hash> hashes) {
    List<WriteRequest> requests = new ArrayList<>();
    for (Hash hash : hashes) {
      requests.add(
          WriteRequest.builder()
              .deleteRequest(
                  b ->
                      b.key(
                          Collections.singletonMap(
                              KEY_NAME,
                              AttributeValue.builder().s(keyPrefix + hash.asString()).build())))
              .build());
    }
    return requests;
  }

  @Override
  protected int entitySize(CommitLogEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  @Override
  protected int entitySize(KeyWithType entry) {
    return toProto(entry).getSerializedSize();
  }

  private <T> List<T> fetchPageResult(String table, List<Hash> hashes, Parser<T> parser) {
    Map<Hash, T> map = fetchPage(table, hashes, parser);
    return hashes.stream().map(map::get).collect(Collectors.toList());
  }

  private <T> Map<Hash, T> fetchPage(String table, List<Hash> hashes, Parser<T> parser) {
    List<Map<String, AttributeValue>> keys =
        hashes.stream()
            .map(h -> keyPrefix + h.asString())
            .map(k -> AttributeValue.builder().s(k).build())
            .map(k -> Collections.singletonMap(KEY_NAME, k))
            .collect(Collectors.toList());

    Map<String, KeysAndAttributes> requestItems =
        Collections.singletonMap(
            table,
            KeysAndAttributes.builder().attributesToGet(KEY_NAME, VALUE_NAME).keys(keys).build());

    BatchGetItemResponse response = client.client.batchGetItem(b -> b.requestItems(requestItems));
    if (!response.hasResponses()) {
      return Collections.emptyMap();
    }

    if (response.hasUnprocessedKeys() && !response.unprocessedKeys().isEmpty()) {
      throw new IllegalArgumentException(
          "Requested too many keys, unprocessed keys: " + response.unprocessedKeys());
    }

    List<Map<String, AttributeValue>> items = response.responses().get(table);
    return items.stream()
        .collect(
            Collectors.toMap(
                m -> Hash.of(m.get(KEY_NAME).s().substring(keyPrefix.length())),
                m -> {
                  try {
                    return parser.parse(m.get(VALUE_NAME).b().asByteArray());
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                }));
  }

  private void insert(String table, String key, byte[] data) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(KEY_NAME, AttributeValue.builder().s(keyPrefix + key).build());
    item.put(VALUE_NAME, AttributeValue.builder().b(SdkBytes.fromByteArray(data)).build());
    client.client.putItem(b -> b.tableName(table).item(item));
  }

  private <T> void batchWrite(
      String tableName, List<T> entries, Function<T, Hash> id, Function<T, byte[]> serializer) {
    if (entries.isEmpty()) {
      return;
    }

    List<WriteRequest> requests = new ArrayList<>();
    for (T entry : entries) {
      Map<String, AttributeValue> item = new HashMap<>();
      String key = keyPrefix + id.apply(entry).asString();
      item.put(KEY_NAME, AttributeValue.builder().s(key).build());
      item.put(
          VALUE_NAME,
          AttributeValue.builder().b(SdkBytes.fromByteArray(serializer.apply(entry))).build());

      if (requests.size() == DYNAMO_BATCH_WRITE_MAX_REQUESTS) {
        client.client.batchWriteItem(
            b -> b.requestItems(Collections.singletonMap(tableName, requests)));
        requests.clear();
      }

      WriteRequest write = WriteRequest.builder().putRequest(b -> b.item(item)).build();
      requests.add(write);
    }
    client.client.batchWriteItem(
        b -> b.requestItems(Collections.singletonMap(tableName, requests)));
  }
}
