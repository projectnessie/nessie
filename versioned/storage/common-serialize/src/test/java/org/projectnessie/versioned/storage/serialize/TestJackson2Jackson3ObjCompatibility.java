/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.versioned.storage.serialize;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.versioned.storage.common.objtypes.GenericObjTypeMapper.newGenericObjType;
import static org.projectnessie.versioned.storage.common.objtypes.JsonObj.json;
import static org.projectnessie.versioned.storage.common.objtypes.UpdateableObj.extractVersionToken;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.util.Compressions.uncompress;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.config.ImmutableAdlsOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsOptions;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.catalog.service.config.ImmutableCatalogConfig;
import org.projectnessie.catalog.service.config.ImmutableLakehouseConfig;
import org.projectnessie.catalog.service.objtypes.EntityObj;
import org.projectnessie.catalog.service.objtypes.EntitySnapshotObj;
import org.projectnessie.catalog.service.objtypes.LakehouseConfigObj;
import org.projectnessie.catalog.service.objtypes.SignerKey;
import org.projectnessie.catalog.service.objtypes.SignerKeysObj;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.GenericObj;
import org.projectnessie.versioned.storage.common.objtypes.ImmutableGenericObj;
import org.projectnessie.versioned.storage.common.objtypes.JsonObj;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.commontests.objtypes.AnotherTestObj;
import org.projectnessie.versioned.storage.commontests.objtypes.ImmutableJsonTestBean;
import org.projectnessie.versioned.storage.commontests.objtypes.SimpleTestObj;
import org.projectnessie.versioned.storage.commontests.objtypes.VersionedTestObj;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.dataformat.smile.SmileMapper;

public class TestJackson2Jackson3ObjCompatibility {

  @Test
  void jackson3JsonObjSmileUsesSingleUnwrappedTypeProperty() {
    JsonObj obj = jsonObj();
    AtomicReference<Compression> compression = new AtomicReference<>();
    byte[] smile = SmileSerialization.serializeObj(obj, compression::set);
    byte[] uncompressedSmile = uncompress(compression.get(), smile);

    int typeProperties = 0;
    int objectProperties = 0;
    int wrappedBeanProperties = 0;
    try (JsonParser parser = new SmileMapper().createParser(uncompressedSmile)) {
      JsonToken token;
      while ((token = parser.nextToken()) != null) {
        if (token == JsonToken.PROPERTY_NAME) {
          String fieldName = parser.currentName();
          if ("t".equals(fieldName)) {
            typeProperties++;
          } else if ("o".equals(fieldName)) {
            objectProperties++;
          } else if ("bean".equals(fieldName)) {
            wrappedBeanProperties++;
          }
        }
      }
    }

    assertThat(typeProperties).isOne();
    assertThat(objectProperties).isOne();
    assertThat(wrappedBeanProperties).isZero();
  }

  @ParameterizedTest
  @MethodSource("customObjs")
  void legacyObjProtoWithEmbeddedVersionTokenCanBeReadAndReserializedByJackson3(Obj obj)
      throws Exception {
    byte[] legacyBytes = LegacyJackson2ObjSerialization.serializeObj(obj, true);

    Obj jackson3Read = deserializeObj(obj.id(), 0L, legacyBytes, null);
    assertObj(jackson3Read, obj);

    byte[] jackson3Bytes = serializeObj(jackson3Read, Integer.MAX_VALUE, Integer.MAX_VALUE, true);
    Obj jackson3Reread = deserializeObj(obj.id(), 0L, jackson3Bytes, null);
    assertObj(jackson3Reread, obj);

    if (legacyJackson2CanReadJackson3Bytes(obj)) {
      Obj legacyRead =
          LegacyJackson2ObjSerialization.deserializeObj(obj.id(), 0L, jackson3Bytes, null);
      assertObj(legacyRead, obj);
    }
  }

  @ParameterizedTest
  @MethodSource("updateableObjs")
  void legacyObjProtoWithoutVersionTokenUsesExternalStorageMetadata(UpdateableObj obj)
      throws Exception {
    long externalReferenced = 42L;
    String externalVersionToken = "external-version-token";
    byte[] legacyBytes = LegacyJackson2ObjSerialization.serializeObj(obj, false);

    Obj jackson3Read =
        deserializeObj(obj.id(), externalReferenced, legacyBytes, externalVersionToken);
    assertObjExceptVersionToken(jackson3Read, obj.withReferenced(externalReferenced));
    assertThat(extractVersionToken(jackson3Read)).contains(externalVersionToken);

    byte[] jackson3Bytes = serializeObj(jackson3Read, Integer.MAX_VALUE, Integer.MAX_VALUE, false);
    Obj jackson3Reread =
        deserializeObj(obj.id(), externalReferenced, jackson3Bytes, externalVersionToken);
    assertObjExceptVersionToken(jackson3Reread, obj.withReferenced(externalReferenced));
    assertThat(extractVersionToken(jackson3Reread)).contains(externalVersionToken);

    if (legacyJackson2CanReadJackson3Bytes(obj)) {
      Obj legacyRead =
          LegacyJackson2ObjSerialization.deserializeObj(
              obj.id(), externalReferenced, jackson3Bytes, externalVersionToken);
      assertObjExceptVersionToken(legacyRead, obj.withReferenced(externalReferenced));
      assertThat(extractVersionToken(legacyRead)).contains(externalVersionToken);
    }
  }

  @ParameterizedTest
  @MethodSource("customObjs")
  void directLegacySmileCanBeReadAndReserializedByJackson3(Obj obj) {
    AtomicReference<Compression> legacyCompression = new AtomicReference<>();
    byte[] legacySmile = LegacyJackson2ObjSerialization.serializeSmile(obj, legacyCompression::set);

    Obj jackson3Read =
        SmileSerialization.deserializeObj(
            obj.id(),
            extractVersionToken(obj).orElse(null),
            legacySmile,
            obj.type(),
            obj.referenced(),
            legacyCompression.get());
    assertObj(jackson3Read, obj);

    AtomicReference<Compression> jackson3Compression = new AtomicReference<>();
    byte[] jackson3Smile = SmileSerialization.serializeObj(jackson3Read, jackson3Compression::set);
    if (legacyJackson2CanReadJackson3Bytes(obj)) {
      Obj legacyRead =
          LegacyJackson2ObjSerialization.deserializeSmile(
              obj.id(),
              extractVersionToken(obj).orElse(null),
              jackson3Smile,
              obj.type(),
              obj.referenced(),
              jackson3Compression.get());
      assertObj(legacyRead, obj);
    }
  }

  @ParameterizedTest
  @MethodSource("unknownGenericObjs")
  void unknownCustomObjTypeFallsBackToGenericObj(GenericObj obj) throws Exception {
    byte[] legacyBytes = LegacyJackson2ObjSerialization.serializeObj(obj, true);

    Obj jackson3Read = deserializeObj(obj.id(), 0L, legacyBytes, null);
    assertThat(jackson3Read).isInstanceOf(GenericObj.class);
    assertObj(jackson3Read, obj);
    assertThat(((GenericObj) jackson3Read).attributes()).containsAllEntriesOf(obj.attributes());

    byte[] jackson3Bytes = serializeObj(jackson3Read, Integer.MAX_VALUE, Integer.MAX_VALUE, true);
    Obj legacyRead =
        LegacyJackson2ObjSerialization.deserializeObj(obj.id(), 0L, jackson3Bytes, null);
    assertThat(legacyRead).isInstanceOf(GenericObj.class);
    assertObj(legacyRead, obj);
    assertThat(((GenericObj) legacyRead).attributes()).containsAllEntriesOf(obj.attributes());
  }

  static Stream<Obj> customObjs() {
    return Stream.of(
        SimpleTestObj.builder().id(randomObjId()).text("simple").build(),
        AnotherTestObj.builder()
            .id(randomObjId())
            .referenced(123L)
            .parent(randomObjId())
            .text("payload ".repeat(2_000))
            .number(42.42d)
            .map(Map.of("k1", "v1", "k2", "v2"))
            .list(List.of("a", "b", "c"))
            .optional("optional")
            .instant(Instant.ofEpochMilli(1234567890L))
            .build(),
        versionedTestObj(),
        jsonObj(),
        lakehouseConfigObj(),
        entityObj(),
        entitySnapshotObj(),
        signerKeysObj());
  }

  static Stream<UpdateableObj> updateableObjs() {
    return Stream.of(
        versionedTestObj(),
        lakehouseConfigObj(),
        entityObj(),
        entitySnapshotObj(),
        signerKeysObj());
  }

  static Stream<GenericObj> unknownGenericObjs() {
    ObjId id = randomObjId();
    return Stream.of(
        ImmutableGenericObj.builder()
            .id(id)
            .type(newGenericObjType("future-type"))
            .putAttributes("string", "value")
            .putAttributes("integer", 42)
            .putAttributes("double", 42.42d)
            .putAttributes("nested", Map.of("inner", "value"))
            .putAttributes("array", asList("a", "b", "c"))
            .putAttributes("versionToken", "future-version")
            .build());
  }

  private static VersionedTestObj versionedTestObj() {
    return VersionedTestObj.builder()
        .id(randomObjId())
        .referenced(321L)
        .someValue("versioned-value")
        .versionToken("version-token")
        .build();
  }

  private static JsonObj jsonObj() {
    return json(
        randomObjId(),
        456L,
        ImmutableJsonTestBean.builder()
            .parent(randomObjId())
            .text("json")
            .number(42)
            .map(Map.of("json", "value"))
            .list(List.of("x", "y"))
            .instant(Instant.ofEpochMilli(123456789L))
            .optional(Optional.of("present"))
            .build());
  }

  private static SignerKeysObj signerKeysObj() {
    Instant now = Instant.parse("2026-07-07T12:00:00Z");
    SignerKey key =
        SignerKey.builder()
            .name("key-1")
            .secretKey("0123456789abcdef0123456789abcdef".getBytes(UTF_8))
            .creationTime(now)
            .rotationTime(now.plusSeconds(3600))
            .expirationTime(now.plusSeconds(7200))
            .build();
    return SignerKeysObj.builder().versionToken("signer-version").addSignerKeys(key).build();
  }

  private static LakehouseConfigObj lakehouseConfigObj() {
    return LakehouseConfigObj.builder()
        .versionToken("lakehouse-version")
        .lakehouseConfig(
            ImmutableLakehouseConfig.builder()
                .catalog(ImmutableCatalogConfig.builder().build())
                .s3(ImmutableS3Options.builder().build())
                .gcs(ImmutableGcsOptions.builder().build())
                .adls(ImmutableAdlsOptions.builder().build())
                .build())
        .build();
  }

  private static EntityObj entityObj() {
    return EntityObj.builder()
        .id(randomObjId())
        .versionToken("entity-version")
        .entity(catalogTable())
        .build();
  }

  private static EntitySnapshotObj entitySnapshotObj() {
    NessieTable table = catalogTable();
    NessieTableSnapshot snapshot =
        NessieTableSnapshot.builder()
            .id(NessieId.nessieIdFromLongs(1L, 2L, 3L, 4L))
            .entity(table)
            .lastUpdatedTimestamp(Instant.parse("2026-07-07T12:34:56Z"))
            .build();
    EntitySnapshotObj.Builder builder = EntitySnapshotObj.builder();
    builder.id(randomObjId());
    builder.versionToken("snapshot-version");
    builder.taskState(
        TaskState.retryableErrorState(Instant.ofEpochMilli(5678L), "retry", "E_RETRY"));
    builder.entity(randomObjId());
    builder.snapshot(snapshot);
    return builder.build();
  }

  private static NessieTable catalogTable() {
    return NessieTable.builder()
        .nessieContentId("catalog-content-id")
        .icebergUuid("00000000-0000-0000-0000-000000000001")
        .tableFormat(TableFormat.ICEBERG)
        .createdTimestamp(Instant.parse("2026-07-07T00:00:00Z"))
        .build();
  }

  private static void assertObj(Obj actual, Obj expected) {
    assertObjExceptVersionToken(actual, expected);
    assertThat(extractVersionToken(actual)).isEqualTo(extractVersionToken(expected));
    assertThat(actual).isEqualTo(expected);
  }

  private static void assertObjExceptVersionToken(Obj actual, Obj expected) {
    assertThat(actual.id()).isEqualTo(expected.id());
    assertThat(actual.type()).isEqualTo(expected.type());
    assertThat(actual.referenced()).isEqualTo(expected.referenced());
    assertPayload(actual, expected);
  }

  private static void assertPayload(Obj actual, Obj expected) {
    if (actual instanceof VersionedTestObj actualVersioned
        && expected instanceof VersionedTestObj expectedVersioned) {
      assertThat(actualVersioned.someValue()).isEqualTo(expectedVersioned.someValue());
    } else if (actual instanceof SignerKeysObj actualSignerKeys
        && expected instanceof SignerKeysObj expectedSignerKeys) {
      assertThat(actualSignerKeys.signerKeys()).isEqualTo(expectedSignerKeys.signerKeys());
    } else if (actual instanceof LakehouseConfigObj actualLakehouseConfig
        && expected instanceof LakehouseConfigObj expectedLakehouseConfig) {
      assertThat(actualLakehouseConfig.lakehouseConfig())
          .isEqualTo(expectedLakehouseConfig.lakehouseConfig());
    } else if (actual instanceof EntityObj actualEntity
        && expected instanceof EntityObj expectedEntity) {
      assertThat(actualEntity.entity()).isEqualTo(expectedEntity.entity());
    } else if (actual instanceof EntitySnapshotObj actualEntitySnapshot
        && expected instanceof EntitySnapshotObj expectedEntitySnapshot) {
      assertThat(actualEntitySnapshot.entity()).isEqualTo(expectedEntitySnapshot.entity());
      assertThat(actualEntitySnapshot.snapshot()).isEqualTo(expectedEntitySnapshot.snapshot());
      assertThat(actualEntitySnapshot.taskState()).isEqualTo(expectedEntitySnapshot.taskState());
    } else {
      assertThat(actual).isEqualTo(expected);
    }
  }

  private static boolean legacyJackson2CanReadJackson3Bytes(Obj obj) {
    return !(obj instanceof JsonObj)
        && !(obj instanceof LakehouseConfigObj)
        && !(obj instanceof EntityObj)
        && !(obj instanceof EntitySnapshotObj)
        && !(obj instanceof SignerKeysObj);
  }
}
