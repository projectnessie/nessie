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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.toProto;
import static org.projectnessie.versioned.persist.dynamodb.Tables.KEY_NAME;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_COMMIT_LOG;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_GLOBAL_LOG;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_GLOBAL_POINTER;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_KEY_LISTS;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_REF_LOG;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_REPO_DESC;
import static org.projectnessie.versioned.persist.dynamodb.Tables.VALUE_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.versioned.BackendLimitExceededException;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization;
import org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.Parser;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry;
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
  private static final int DYNAMO_MAX_ITEM_SIZE = 375 * 1024;

  private static final char PREFIX_SEPARATOR = ':';
  private final DynamoDatabaseClient client;

  private final String keyPrefix;
  private final Map<String, AttributeValue> globalPointerKeyMap;

  public DynamoDatabaseAdapter(
      NonTransactionalDatabaseAdapterConfig config,
      DynamoDatabaseClient c,
      StoreWorker<?, ?, ?> storeWorker) {
    super(config, storeWorker);

    Objects.requireNonNull(
        c, "Requires a non-null DynamoDatabaseClient from DynamoDatabaseAdapterConfig");

    client = c;

    String keyPrefix = config.getRepositoryId();
    if (keyPrefix.indexOf(PREFIX_SEPARATOR) >= 0) {
      throw new IllegalArgumentException("Invalid key prefix: " + keyPrefix);
    }
    this.keyPrefix = keyPrefix + PREFIX_SEPARATOR;

    globalPointerKeyMap =
        singletonMap(KEY_NAME, AttributeValue.builder().s(this.keyPrefix).build());
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
  public void eraseRepo() {
    client.client.deleteItem(
        b -> b.tableName(TABLE_GLOBAL_POINTER).key(globalPointerKeyMap).build());

    try (BatchDelete batchDelete = new BatchDelete()) {
      Stream.of(TABLE_GLOBAL_LOG, TABLE_COMMIT_LOG, TABLE_KEY_LISTS, TABLE_REF_LOG)
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
                                  .forEach(key -> batchDelete.add(table, key))));
    }
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
        singletonMap(KEY_NAME, AttributeValue.builder().s(keyPrefix + id).build());

    GetItemResponse response = client.client.getItem(b -> b.tableName(table).key(key));
    if (!response.hasItem()) {
      return null;
    }

    Map<String, AttributeValue> attributes = response.item();
    SdkBytes bytes = attributes.get(VALUE_NAME).b();
    return bytes.asByteArray();
  }

  @Override
  protected GlobalStatePointer doFetchGlobalPointer(NonTransactionalOperationContext ctx) {
    return loadById(TABLE_GLOBAL_POINTER, "", GlobalStatePointer::parseFrom);
  }

  @Override
  protected CommitLogEntry doFetchFromCommitLog(NonTransactionalOperationContext ctx, Hash hash) {
    return loadById(TABLE_COMMIT_LOG, hash, ProtoSerialization::protoToCommitLogEntry);
  }

  @Override
  protected GlobalStateLogEntry doFetchFromGlobalLog(
      NonTransactionalOperationContext ctx, Hash id) {
    return loadById(TABLE_GLOBAL_LOG, id, GlobalStateLogEntry::parseFrom);
  }

  @Override
  protected List<CommitLogEntry> doFetchMultipleFromCommitLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPageResult(TABLE_COMMIT_LOG, hashes, ProtoSerialization::protoToCommitLogEntry);
  }

  @Override
  protected List<GlobalStateLogEntry> doFetchPageFromGlobalLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPageResult(TABLE_GLOBAL_LOG, hashes, GlobalStateLogEntry::parseFrom);
  }

  @Override
  protected Stream<KeyListEntity> doFetchKeyLists(
      NonTransactionalOperationContext ctx, List<Hash> keyListsIds) {
    Map<Hash, KeyList> map =
        fetchPage(TABLE_KEY_LISTS, keyListsIds, ProtoSerialization::protoToKeyList);
    return keyListsIds.stream()
        .map(h -> map.containsKey(h) ? KeyListEntity.of(h, map.get(h)) : null);
  }

  @Override
  protected void doWriteGlobalCommit(
      NonTransactionalOperationContext ctx, GlobalStateLogEntry entry) {
    insert(TABLE_GLOBAL_LOG, Hash.of(entry.getId()).asString(), entry.toByteArray());
  }

  @Override
  protected void doWriteIndividualCommit(
      NonTransactionalOperationContext ctx, CommitLogEntry entry) {
    insert(TABLE_COMMIT_LOG, entry.getHash().asString(), toProto(entry).toByteArray());
  }

  @Override
  protected void unsafeWriteGlobalPointer(
      NonTransactionalOperationContext ctx, GlobalStatePointer pointer) {
    insert(TABLE_GLOBAL_POINTER, "", pointer.toByteArray());
  }

  @Override
  protected void doWriteMultipleCommits(
      NonTransactionalOperationContext ctx, List<CommitLogEntry> entries) {
    batchWrite(
        TABLE_COMMIT_LOG,
        entries,
        CommitLogEntry::getHash,
        e -> ProtoSerialization.toProto(e).toByteArray());
  }

  @Override
  protected void doWriteKeyListEntities(
      NonTransactionalOperationContext ctx, List<KeyListEntity> newKeyListEntities) {
    batchWrite(
        TABLE_KEY_LISTS,
        newKeyListEntities,
        KeyListEntity::getId,
        e -> ProtoSerialization.toProto(e.getKeys()).toByteArray());
  }

  @Override
  protected boolean doGlobalPointerCas(
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
                      singletonMap(
                          VALUE_NAME,
                          ExpectedAttributeValue.builder().value(expectedBytes).build()))
                  .attributeUpdates(
                      singletonMap(
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
  protected void doCleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Optional<Hash> globalHead,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists,
      Hash refLogId) {
    try (BatchDelete batchDelete = new BatchDelete()) {
      branchCommits.forEach(h -> batchDelete.add(TABLE_COMMIT_LOG, h));
      newKeyLists.forEach(h -> batchDelete.add(TABLE_KEY_LISTS, h));
      globalHead.ifPresent(h -> batchDelete.add(TABLE_GLOBAL_LOG, h));
      batchDelete.add(TABLE_REF_LOG, refLogId);
    }
  }

  @Override
  protected void doCleanUpGlobalLog(
      NonTransactionalOperationContext ctx, Collection<Hash> globalIds) {
    try (BatchDelete batchDelete = new BatchDelete()) {
      globalIds.forEach(h -> batchDelete.add(TABLE_GLOBAL_LOG, h));
    }
  }

  private final class BatchDelete implements AutoCloseable {
    private final Map<String, List<WriteRequest>> requestItems = new HashMap<>();
    private int requests;

    void add(String table, Hash hash) {
      add(table, AttributeValue.builder().s(keyPrefix + hash.asString()).build());
    }

    void add(String table, AttributeValue key) {
      requestItems
          .computeIfAbsent(table, t -> new ArrayList<>())
          .add(
              WriteRequest.builder()
                  .deleteRequest(b -> b.key(singletonMap(KEY_NAME, key)))
                  .build());
      requests++;

      if (requests == DYNAMO_BATCH_WRITE_MAX_REQUESTS) {
        close();
      }
    }

    // close() is actually a flush, implementing AutoCloseable for easier use of BatchDelete using
    // try-with-resources.
    @Override
    public void close() {
      if (requests > 0) {
        client.client.batchWriteItem(b -> b.requestItems(requestItems));
        requestItems.clear();
        requests = 0;
      }
    }
  }

  @Override
  protected RepoDescription doFetchRepositoryDescription(NonTransactionalOperationContext ctx) {
    return loadById(TABLE_REPO_DESC, "", ProtoSerialization::protoToRepoDescription);
  }

  @Override
  protected boolean doTryUpdateRepositoryDescription(
      NonTransactionalOperationContext ctx, RepoDescription expected, RepoDescription updateTo) {
    AttributeValue updateToBytes =
        AttributeValue.builder().b(SdkBytes.fromByteArray(toProto(updateTo).toByteArray())).build();
    try {
      if (expected != null) {
        AttributeValue expectedBytes =
            AttributeValue.builder()
                .b(SdkBytes.fromByteArray(toProto(expected).toByteArray()))
                .build();
        client.client.updateItem(
            b ->
                b.tableName(TABLE_REPO_DESC)
                    .key(globalPointerKeyMap)
                    .expected(
                        singletonMap(
                            VALUE_NAME,
                            ExpectedAttributeValue.builder().value(expectedBytes).build()))
                    .attributeUpdates(
                        singletonMap(
                            VALUE_NAME,
                            AttributeValueUpdate.builder()
                                .action(AttributeAction.PUT)
                                .value(updateToBytes)
                                .build())));
      } else {
        client.client.putItem(
            b ->
                b.tableName(TABLE_REPO_DESC)
                    .item(
                        ImmutableMap.of(
                            KEY_NAME, globalPointerKeyMap.get(KEY_NAME), VALUE_NAME, updateToBytes))
                    .conditionExpression(String.format("attribute_not_exists(%s)", VALUE_NAME)));
      }
      return true;
    } catch (ConditionalCheckFailedException e) {
      return false;
    }
  }

  @Override
  protected int maxEntitySize() {
    return DYNAMO_MAX_ITEM_SIZE;
  }

  @Override
  protected int entitySize(CommitLogEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  @Override
  protected int entitySize(KeyListEntry entry) {
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
            .map(k -> singletonMap(KEY_NAME, k))
            .collect(Collectors.toList());

    Map<String, KeysAndAttributes> requestItems =
        singletonMap(
            table,
            KeysAndAttributes.builder().attributesToGet(KEY_NAME, VALUE_NAME).keys(keys).build());

    BatchGetItemResponse response = client.client.batchGetItem(b -> b.requestItems(requestItems));
    if (!response.hasResponses()) {
      return emptyMap();
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
        client.client.batchWriteItem(b -> b.requestItems(singletonMap(tableName, requests)));
        requests.clear();
      }

      WriteRequest write = WriteRequest.builder().putRequest(b -> b.item(item)).build();
      requests.add(write);
    }
    client.client.batchWriteItem(b -> b.requestItems(singletonMap(tableName, requests)));
  }

  @Override
  protected void doWriteRefLog(NonTransactionalOperationContext ctx, RefLogEntry entry) {
    insert(TABLE_REF_LOG, Hash.of(entry.getRefLogId()).asString(), entry.toByteArray());
  }

  @Override
  protected RefLog doFetchFromRefLog(NonTransactionalOperationContext ctx, Hash refLogId) {
    if (refLogId == null) {
      // set the current head as refLogId
      refLogId = Hash.of(fetchGlobalPointer(ctx).getRefLogId());
    }
    return loadById(TABLE_REF_LOG, refLogId, ProtoSerialization::protoToRefLog);
  }

  @Override
  protected List<RefLog> doFetchPageFromRefLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPageResult(TABLE_REF_LOG, hashes, ProtoSerialization::protoToRefLog);
  }
}
