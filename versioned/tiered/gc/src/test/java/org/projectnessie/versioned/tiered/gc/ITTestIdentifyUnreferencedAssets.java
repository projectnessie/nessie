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
package org.projectnessie.versioned.tiered.gc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.dynamodb.DynamoStore;
import org.projectnessie.versioned.dynamodb.DynamoStoreConfig;
import org.projectnessie.versioned.dynamodb.LocalDynamoDB;
import org.projectnessie.versioned.gc.IdentifyUnreferencedAssets;
import org.projectnessie.versioned.gc.core.AssetKey;
import org.projectnessie.versioned.gc.core.AssetKeyConverter;
import org.projectnessie.versioned.gc.core.CategorizedValue;
import org.projectnessie.versioned.impl.TieredVersionStore;
import org.projectnessie.versioned.store.HasId;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tests.CommitBuilder;
import org.projectnessie.versioned.tiered.Value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

import software.amazon.awssdk.regions.Region;

@ExtendWith(LocalDynamoDB.class)
public class ITTestIdentifyUnreferencedAssets {
  private static final long FIVE_DAYS_IN_PAST_MICROS = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis())
      - TimeUnit.DAYS.toMicros(5);
  private static final long TWO_HOURS_IN_PAST_MICROS = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis())
      - TimeUnit.HOURS.toMicros(2);

  private static final long ONE_DAY_OLD_MICROS = TimeUnit.DAYS.toMicros(1);
  private static final long ONE_HOUR_OLD_MICROS = TimeUnit.HOURS.toMicros(1);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private StoreWorker<DummyValue,String> helper;
  private DtAdjustingStore store;
  private TieredVersionStore<DummyValue, String> versionStore;

  @Test
  public void run() throws Exception {
    // commit one asset on main branch.
    BranchName main = BranchName.of("main");

    // create an old commit referencing both unique and non-unique assets.
    //The unique asset should be identified by the gc policy below since they are older than 1 day.
    store.setOverride(FIVE_DAYS_IN_PAST_MICROS);
    commit().put("k1", new DummyValue().add(-3).add(0).add(100)).withMetadata("cOld").toBranch(main);

    // work beyond slop but within gc allowed age.
    store.setOverride(TWO_HOURS_IN_PAST_MICROS);
    // create commits that have time-valid assets. Create more commits than ParentList.MAX_PARENT_LIST to confirm recursion.
    for (int i = 0; i < 55; i++) {
      commit().put("k1", new DummyValue().add(i).add(i + 100)).withMetadata("c2").toBranch(main);
    }

    // create a new branch, commit two assets, then delete the branch.
    BranchName toBeDeleted = BranchName.of("toBeDeleted");
    versionStore.create(toBeDeleted, Optional.empty());
    Hash h = commit().put("k1", new DummyValue().add(-1).add(-2)).withMetadata("c1").toBranch(toBeDeleted);
    versionStore.delete(toBeDeleted, Optional.of(h));

    store.clearOverride();
    {
      // Create a dangling value to ensure that the slop factor avoids deletion of the assets of this otherwise dangling value.
      save(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()), new DummyValue().add(-50).add(-51));

      // create a dangling value that should be cleaned up.
      save(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()) - TimeUnit.DAYS.toMicros(2), new DummyValue().add(-60).add(-61));
    }

    SparkSession spark = SparkSession
        .builder()
        .appName("test-nessie-gc-collection")
        .master("local[2]")
        .getOrCreate();

    // now confirm that the unreferenced assets are marked for deletion. These are found based
    // on the no-longer referenced commit as well as the old commit.
    GcOptions options = ImmutableGcOptions.builder()
        .bloomFilterCapacity(10_000_000)
        .maxAgeMicros(ONE_DAY_OLD_MICROS)
        .timeSlopMicros(ONE_HOUR_OLD_MICROS)
        .build();
    IdentifyUnreferencedValues<DummyValue> app = new IdentifyUnreferencedValues<>(helper, new DynamoSupplier(), spark, options);
    Dataset<CategorizedValue> values = app.identify();

    IdentifyUnreferencedAssets<DummyValue> ident = new IdentifyUnreferencedAssets<DummyValue>(helper, new DummyAssetKeySerializer(),
        new DummeyAssetKeyConverter(), values, spark);
    Dataset<IdentifyUnreferencedAssets.UnreferencedItem> items = ident.identify();
    Set<String> unreferencedItems = items.collectAsList().stream().map(IdentifyUnreferencedAssets.UnreferencedItem::getName)
        .collect(Collectors.toSet());
    assertThat(unreferencedItems, containsInAnyOrder("-1", "-2", "-3", "-60", "-61"));
  }

  private void save(long microsDt, DummyValue value) {
    SaveOp<Value> saveOp = new SaveOp<Value>(ValueType.VALUE, Id.generateRandom()) {
      @Override
      public void serialize(Value consumer) {
        try {
          consumer.dt(microsDt)
              .id(Id.generateRandom())
              .value(ByteString.copyFrom(MAPPER.writeValueAsBytes(value)));
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }

      }
    };
    store.put(saveOp, Optional.empty());
  }

  private static class DynamoSupplier implements Supplier<Store>, Serializable {

    private static final long serialVersionUID = 5030232198230089450L;

    static DynamoStore createStore() throws URISyntaxException {
      return new DynamoStore(DynamoStoreConfig.builder().endpoint(new URI("http://localhost:8000"))
          .region(Region.US_WEST_2).build());
    }

    @Override
    public Store get() {
      Store store;
      try {
        store = createStore();
        store.start();
        return store;
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @BeforeEach
  void before() throws Exception {
    helper = new StoreW();
    store = new DtAdjustingStore(DynamoSupplier.createStore());
    store.start();
    versionStore = new TieredVersionStore<>(helper, store, true);
    versionStore.create(BranchName.of("main"), Optional.empty());
  }

  @AfterEach
  void after() {
    store.deleteTables();
    store.close();
    helper = null;
    versionStore = null;
  }


  private CommitBuilder<DummyValue, String> commit() {
    return new CommitBuilder<DummyValue, String>(versionStore);
  }

  private static class StoreW implements StoreWorker<DummyValue, String> {
    @Override
    public Serializer<DummyValue> getValueSerializer() {
      return new DummyValueSerializer();
    }

    @Override
    public Serializer<String> getMetadataSerializer() {
      return StringSerializer.getInstance();
    }
  }

  private static class JsonSerializer<T> implements Serializer<T>, Serializable {

    private static final long serialVersionUID = 4052464280276785753L;

    private final Class<T> clazz;

    public JsonSerializer(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public ByteString toBytes(T value) {
      try {
        return ByteString.copyFrom(MAPPER.writer().writeValueAsBytes(value));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public T fromBytes(ByteString bytes) {
      try {
        return MAPPER.reader().readValue(bytes.toByteArray(), clazz);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  private static class DummyValueSerializer extends JsonSerializer<DummyValue> implements Serializable {

    public DummyValueSerializer() {
      super(DummyValue.class);
    }

  }

  // annoying games w/ Java generics.
  private static class DummyAssetKeySerializer implements Serializer<AssetKey>, Serializable {
    private final Serializer<DummyAsset> delegate = new JsonSerializer<>(DummyAsset.class);

    @Override
    public ByteString toBytes(AssetKey value) {
      return delegate.toBytes((DummyAsset) value);
    }

    @Override
    public AssetKey fromBytes(ByteString bytes) {
      return delegate.fromBytes(bytes);
    }
  }

  private static class DummeyAssetKeyConverter implements AssetKeyConverter<DummyValue>, Serializable {

    @Override
    public Stream<? extends AssetKey> getAssetKeys(DummyValue value) {
      return value.assets.stream();
    }

  }

  private static class DummyValue implements HasId {

    private final Id id;
    private final List<DummyAsset> assets;

    @JsonCreator
    public DummyValue(@JsonProperty("id") byte[] id, @JsonProperty("assets") List<DummyAsset> assets) {
      super();
      this.id = Id.of(id);
      this.assets = assets;
    }

    public DummyValue() {
      this.id = Id.generateRandom();
      this.assets = new ArrayList<>();
    }

    @JsonIgnore
    @Override
    public Id getId() {
      return id;
    }

    @JsonProperty("id")
    public byte[] idAsBytes() {
      return id.toBytes();
    }

    public DummyValue add(int id) {
      assets.add(new DummyAsset(id));
      return this;
    }

    public List<DummyAsset> getAssets() {
      return assets;
    }
  }

  private static class DummyAsset extends AssetKey {

    private int id;

    @JsonCreator
    public DummyAsset(@JsonProperty("id") int id) {
      this.id = id;
    }

    public DummyAsset() {
    }


    public int getId() {
      return id;
    }

    @Override
    public CompletableFuture<Boolean> delete() {
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public List<String> toReportableName() {
      return Arrays.asList(Integer.toString(id));
    }

    @Override
    public boolean equals(Object other) {
      return other != null && other instanceof DummyAsset && ((DummyAsset)other).id == id;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(id);
    }

  }
}
