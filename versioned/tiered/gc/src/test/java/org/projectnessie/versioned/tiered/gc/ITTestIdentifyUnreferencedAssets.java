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

import java.io.Serializable;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
import org.projectnessie.versioned.SerializerWithPayload;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.dynamodb.LocalDynamoDB;
import org.projectnessie.versioned.gc.AssetKeyConverter;
import org.projectnessie.versioned.gc.CategorizedValue;
import org.projectnessie.versioned.gc.GcTestUtils;
import org.projectnessie.versioned.gc.IdentifyUnreferencedAssets;
import org.projectnessie.versioned.impl.TieredVersionStore;
import org.projectnessie.versioned.store.HasId;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tests.CommitBuilder;
import org.projectnessie.versioned.tiered.Value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

@ExtendWith(LocalDynamoDB.class)
public class ITTestIdentifyUnreferencedAssets {
  private static final long FIVE_DAYS_IN_PAST_MICROS = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis())
      - TimeUnit.DAYS.toMicros(5);
  private static final long TWO_HOURS_IN_PAST_MICROS = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis())
      - TimeUnit.HOURS.toMicros(2);

  private static final long ONE_DAY_OLD_MICROS = TimeUnit.DAYS.toMicros(1);
  private static final long ONE_HOUR_OLD_MICROS = TimeUnit.HOURS.toMicros(1);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private StoreWorker<DummyValue,String, StringSerializer.TestEnum> helper;
  private DtAdjustingStore store;
  private TieredVersionStore<DummyValue, String, StringSerializer.TestEnum> versionStore;

  @Test
  public void run() throws Exception {
    Set<DummyValue> expectedValues = new HashSet<>(59);
    Set<DummyValue> expectedReferencedValues = new HashSet<>(59);
    // commit one asset on main branch.
    BranchName main = BranchName.of("main");

    // create an old commit referencing both unique and non-unique assets.
    //The unique asset should be identified by the gc policy below since they are older than 1 day.
    store.setOverride(FIVE_DAYS_IN_PAST_MICROS);
    DummyValue dv = new DummyValue().add(-3).add(0).add(100);
    expectedValues.add(dv);
    commit().put("k1", dv).withMetadata("cOld").toBranch(main);

    // work beyond slop but within gc allowed age.
    store.setOverride(TWO_HOURS_IN_PAST_MICROS);
    // create commits that have time-valid assets. Create more commits than ParentList.MAX_PARENT_LIST to confirm recursion.
    for (int i = 0; i < 55; i++) {
      dv = new DummyValue().add(i).add(i + 100);
      expectedValues.add(dv);
      expectedReferencedValues.add(dv);
      commit().put("k1", dv).withMetadata("c2").toBranch(main);
    }

    // create a new branch, commit two assets, then delete the branch.
    BranchName toBeDeleted = BranchName.of("toBeDeleted");
    versionStore.create(toBeDeleted, Optional.empty());
    dv = new DummyValue().add(-1).add(-2);
    expectedValues.add(dv);
    Hash h = commit().put("k1", dv).withMetadata("c1").toBranch(toBeDeleted);
    versionStore.delete(toBeDeleted, Optional.of(h));

    store.clearOverride();
    {
      // Create a dangling value to ensure that the slop factor avoids deletion of the assets of this otherwise dangling value.
      dv = new DummyValue().add(-50).add(-51);
      expectedValues.add(dv);
      expectedReferencedValues.add(dv);
      save(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()), dv);

      // create a dangling value that should be cleaned up.
      dv = new DummyValue().add(-60).add(-61);
      expectedValues.add(dv);
      save(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()) - TimeUnit.DAYS.toMicros(2), dv);
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
    IdentifyUnreferencedValues<DummyValue> identifyValues = new IdentifyUnreferencedValues<>(helper, new DynamoSupplier(), spark, options,
        Clock.systemUTC());
    Dataset<CategorizedValue> values = identifyValues.identify();

    // test to make sure values are correct and correctly referenced.
    List<CategorizedValue> valuesList = values.collectAsList();
    Set<DummyValue> actualReferencedValues = valuesList.stream().filter(CategorizedValue::isReferenced).map(CategorizedValue::getData)
        .map(x -> helper.getValueSerializer().fromBytes(ByteString.copyFrom(x))).collect(Collectors.toSet());
    Set<DummyValue> actualValues = valuesList.stream().map(CategorizedValue::getData)
        .map(x -> helper.getValueSerializer().fromBytes(ByteString.copyFrom(x))).collect(Collectors.toSet());
    assertThat(actualReferencedValues, containsInAnyOrder(expectedReferencedValues.toArray(new DummyValue[0])));
    assertThat(actualValues, containsInAnyOrder(expectedValues.toArray(new DummyValue[0])));

    IdentifyUnreferencedAssets<DummyValue, GcTestUtils.DummyAsset> identifyAssets = new IdentifyUnreferencedAssets<>(
        helper.getValueSerializer(), new GcTestUtils.DummyAssetKeySerializer(), new DummyAssetConverter(), v -> true, spark);
    Dataset<IdentifyUnreferencedAssets.UnreferencedItem> items = identifyAssets.identify(values);
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


  private CommitBuilder<DummyValue, String, StringSerializer.TestEnum> commit() {
    return new CommitBuilder<DummyValue, String, StringSerializer.TestEnum>(versionStore);
  }

  private static class StoreW implements StoreWorker<DummyValue, String, StringSerializer.TestEnum> {
    @Override
    public SerializerWithPayload<DummyValue, StringSerializer.TestEnum> getValueSerializer() {
      return new DummyValueSerializer();
    }

    @Override
    public Serializer<String> getMetadataSerializer() {
      return StringSerializer.getInstance();
    }
  }

  private static class DummyValueSerializer extends GcTestUtils.JsonSerializer<DummyValue> implements Serializable,
      SerializerWithPayload<DummyValue, StringSerializer.TestEnum> {

    public DummyValueSerializer() {
      super(DummyValue.class);
    }

    @Override
    public Byte getPayload(DummyValue value) {
      return 0;
    }

    @Override
    public StringSerializer.TestEnum getType(Byte payload) {
      return StringSerializer.TestEnum.NO;
    }
  }

  private static class DummyAssetConverter implements AssetKeyConverter<DummyValue, GcTestUtils.DummyAsset>, Serializable {

    @Override
    public Stream<GcTestUtils.DummyAsset> apply(DummyValue value) {
      return value.assets.stream();
    }
  }

  private static class DummyValue implements HasId {

    private final Id id;
    private final List<GcTestUtils.DummyAsset> assets;

    @JsonCreator
    public DummyValue(@JsonProperty("id") byte[] id, @JsonProperty("assets") List<GcTestUtils.DummyAsset> assets) {
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
      assets.add(new GcTestUtils.DummyAsset(id));
      return this;
    }

    public List<GcTestUtils.DummyAsset> getAssets() {
      return assets;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DummyValue that = (DummyValue) o;
      return Objects.equals(id, that.id) && Objects.equals(assets, that.assets);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, assets);
    }
  }


}
