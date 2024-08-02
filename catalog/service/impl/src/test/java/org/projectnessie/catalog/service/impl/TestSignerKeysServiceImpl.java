/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.service.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.projectnessie.catalog.service.impl.SignerKeysServiceImpl.NEW_KEY_EXPIRE_AFTER;
import static org.projectnessie.catalog.service.impl.SignerKeysServiceImpl.NEW_KEY_ROTATE_AFTER;
import static org.projectnessie.catalog.service.objtypes.SignerKeysObj.OBJ_ID;
import static org.projectnessie.catalog.service.objtypes.SignerKeysObj.OBJ_TYPE;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.service.objtypes.ImmutableSignerKey;
import org.projectnessie.catalog.service.objtypes.SignerKey;
import org.projectnessie.catalog.service.objtypes.SignerKeysObj;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessiePersistCache;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.threeten.extra.MutableClock;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
@NessiePersistCache // should test w/ persist-cache to exercise custom obj type serialization
public class TestSignerKeysServiceImpl {

  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  protected Instant initialInstant;
  protected MutableClock clock;

  protected SignerKeysServiceImpl service;

  protected Function<SignerKeysObj, Boolean> storeInitialTweak;
  protected BiFunction<SignerKeysObj, SignerKeysObj, Boolean> updateKeysTweak;

  @BeforeEach
  protected void setup() {
    initialInstant = Instant.now();
    clock = MutableClock.of(initialInstant, ZoneId.of("Z"));

    storeInitialTweak = o -> null;
    updateKeysTweak = (c, u) -> null;

    service =
        spy(
            new SignerKeysServiceImpl() {
              @Override
              boolean storeInitial(SignerKeysObj signerKeys) throws ObjTooLargeException {
                Boolean r = storeInitialTweak.apply(signerKeys);
                return r != null ? r : super.storeInitial(signerKeys);
              }

              @Override
              boolean updateKeys(SignerKeysObj keys, SignerKeysObj updated)
                  throws ObjTooLargeException {
                Boolean r = updateKeysTweak.apply(keys, updated);
                return r != null ? r : super.updateKeys(keys, updated);
              }
            });
    service.persist = persist;
    service.clock = clock;
  }

  @Test
  public void lifecycle() throws Exception {
    soft.assertThatThrownBy(() -> persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThat(service.getSignerKey("foo")).isNull();
    soft.assertThatCode(() -> persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class))
        .doesNotThrowAnyException();

    // Notes: NEW_KEY_ROTATE_AFTER is 3 days
    soft.assertThat(NEW_KEY_ROTATE_AFTER).isEqualTo(Duration.of(3, DAYS));
    soft.assertThat(NEW_KEY_EXPIRE_AFTER).isEqualTo(Duration.of(5, DAYS));

    // Key 1 rotate, Key 2 create
    Instant day3 = initialInstant.plus(3, DAYS);
    // Key 1 expire
    Instant day5 = initialInstant.plus(5, DAYS);
    // Key 2 rotate (3 + 3), Key 3 create
    Instant day6 = initialInstant.plus(6, DAYS);
    // Key 2 expire (3 + 5)
    Instant day8 = initialInstant.plus(8, DAYS);
    // Key 3 expire (6 + 3)
    Instant day9 = initialInstant.plus(9, DAYS);

    // @ day 0

    SignerKey key1 = service.currentSignerKey();
    soft.assertThat(key1)
        .isNotNull()
        .extracting(SignerKey::creationTime, SignerKey::rotationTime, SignerKey::expirationTime)
        .containsExactly(
            clock.instant(),
            clock.instant().plus(NEW_KEY_ROTATE_AFTER),
            clock.instant().plus(NEW_KEY_EXPIRE_AFTER));

    // @ right before day 3

    clock.setInstant(day3.minus(1, MILLIS));
    SignerKey key2 = service.currentSignerKey();
    soft.assertThat(key2).isEqualTo(key1);
    SignerKeysObj keys = persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class);
    soft.assertThat(keys.signerKeys().size()).isEqualTo(1);
    soft.assertThat(keys.getSignerKey(key1.name())).isEqualTo(key1);

    // @ day 3

    clock.setInstant(day3);
    key2 = service.currentSignerKey();
    soft.assertThat(key2).isNotEqualTo(key1);
    keys = persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class);
    soft.assertThat(keys.signerKeys()).containsExactly(key1, key2);
    soft.assertThat(keys.getSignerKey(key1.name())).isEqualTo(key1);
    soft.assertThat(keys.getSignerKey(key2.name())).isEqualTo(key2);

    // @ right before day 5

    clock.setInstant(day5.minus(1, MILLIS));
    soft.assertThat(service.currentSignerKey()).isEqualTo(key2);
    keys = persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class);
    soft.assertThat(keys.signerKeys()).containsExactly(key1, key2);
    soft.assertThat(keys.getSignerKey(key1.name())).isEqualTo(key1);
    soft.assertThat(keys.getSignerKey(key2.name())).isEqualTo(key2);

    // @ day 5 (key1 is still there, because no write due to rotation happened)

    clock.setInstant(day5);
    soft.assertThat(service.currentSignerKey()).isEqualTo(key2);
    keys = persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class);
    soft.assertThat(keys.signerKeys()).containsExactly(key1, key2);
    soft.assertThat(keys.getSignerKey(key1.name())).isEqualTo(key1);
    soft.assertThat(keys.getSignerKey(key2.name())).isEqualTo(key2);

    // @ day 6

    clock.setInstant(day6);
    SignerKey key3 = service.currentSignerKey();
    soft.assertThat(key3).isNotEqualTo(key2).isNotEqualTo(key1);
    keys = persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class);
    soft.assertThat(keys.signerKeys()).containsExactly(key2, key3);
    soft.assertThat(keys.getSignerKey(key1.name())).isNull();
    soft.assertThat(keys.getSignerKey(key2.name())).isEqualTo(key2);
    soft.assertThat(keys.getSignerKey(key3.name())).isEqualTo(key3);

    // @ day 8

    clock.setInstant(day8);
    soft.assertThat(service.currentSignerKey()).isEqualTo(key3);
    keys = persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class);
    soft.assertThat(keys.signerKeys()).containsExactly(key2, key3);
    soft.assertThat(keys.getSignerKey(key1.name())).isNull();
    soft.assertThat(keys.getSignerKey(key2.name())).isEqualTo(key2);
    soft.assertThat(keys.getSignerKey(key3.name())).isEqualTo(key3);

    // @ day 9

    clock.setInstant(day9);
    SignerKey key4 = service.currentSignerKey();
    soft.assertThat(key4).isNotEqualTo(key3).isNotEqualTo(key2).isNotEqualTo(key1);
    keys = persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class);
    soft.assertThat(keys.signerKeys()).containsExactly(key3, key4);
    soft.assertThat(keys.getSignerKey(key1.name())).isNull();
    soft.assertThat(keys.getSignerKey(key2.name())).isNull();
    soft.assertThat(keys.getSignerKey(key3.name())).isEqualTo(key3);
    soft.assertThat(keys.getSignerKey(key4.name())).isEqualTo(key4);
  }

  @Test
  public void initialCreateFailed() throws Exception {
    AtomicBoolean storeInitialCalled = new AtomicBoolean(false);

    storeInitialTweak =
        o -> {
          if (storeInitialCalled.compareAndSet(false, true)) {
            return false;
          }
          // continue with real impl
          return null;
        };

    soft.assertThatThrownBy(() -> persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class))
        .isInstanceOf(ObjNotFoundException.class);

    SignerKey key = service.currentSignerKey();
    soft.assertThat(key).isNotNull();

    verify(service, times(2)).storeInitial(any());
  }

  @Test
  public void initialCreateRace() throws Exception {
    AtomicBoolean storeInitialCalled = new AtomicBoolean(false);

    SignerKey raceKey =
        ImmutableSignerKey.of(
            "key-x",
            "01234567890123456789012345678901".getBytes(UTF_8),
            initialInstant,
            initialInstant.plus(NEW_KEY_ROTATE_AFTER),
            initialInstant.plus(NEW_KEY_EXPIRE_AFTER));

    storeInitialTweak =
        o -> {
          if (storeInitialCalled.compareAndSet(false, true)) {
            SignerKeysObj raceObj =
                SignerKeysObj.builder().versionToken("foo").addSignerKey(raceKey).build();
            try {
              assertThat(persist.storeObj(raceObj)).isTrue();
            } catch (ObjTooLargeException e) {
              throw new RuntimeException(e);
            }
          }
          // continue with real impl
          return null;
        };

    soft.assertThatThrownBy(() -> persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class))
        .isInstanceOf(ObjNotFoundException.class);

    SignerKey key = service.currentSignerKey();
    soft.assertThat(key).isEqualTo(raceKey);

    verify(service, times(1)).storeInitial(any());
  }

  @Test
  public void rotateFailed() throws Exception {
    SignerKey key1 = service.currentSignerKey();
    soft.assertThat(key1).isNotNull();

    // force a key rotation / call to updateKeys()
    clock.setInstant(initialInstant.plus(NEW_KEY_ROTATE_AFTER));

    AtomicBoolean updateKeysCalled = new AtomicBoolean(false);

    updateKeysTweak =
        (k, u) -> {
          if (updateKeysCalled.compareAndSet(false, true)) {
            return false;
          }
          // continue with real impl
          return null;
        };

    SignerKey key2 = service.currentSignerKey();
    soft.assertThat(key2).isNotEqualTo(key1);

    SignerKeysObj keys = persist.fetchTypedObj(OBJ_ID, OBJ_TYPE, SignerKeysObj.class);
    soft.assertThat(keys.signerKeys()).containsExactly(key1, key2);

    verify(service, times(2)).updateKeys(any(), any());
  }

  @Test
  public void rotateRace() throws Exception {
    SignerKey key1 = service.currentSignerKey();
    soft.assertThat(key1).isNotNull();

    // force a key rotation / call to updateKeys()
    clock.setInstant(initialInstant.plus(NEW_KEY_ROTATE_AFTER));

    reset(service);

    AtomicBoolean updateKeysCalled = new AtomicBoolean(false);

    Instant raceCreate = initialInstant.plus(NEW_KEY_ROTATE_AFTER);
    SignerKey raceKey =
        ImmutableSignerKey.of(
            "key-x",
            "01234567890123456789012345678901".getBytes(UTF_8),
            raceCreate,
            raceCreate.plus(NEW_KEY_ROTATE_AFTER),
            raceCreate.plus(NEW_KEY_EXPIRE_AFTER));

    updateKeysTweak =
        (k, u) -> {
          if (updateKeysCalled.compareAndSet(false, true)) {
            SignerKeysObj raceObj =
                SignerKeysObj.builder()
                    .from(k)
                    .signerKeys(List.of(key1, raceKey))
                    .versionToken("foo-updated")
                    .build();
            try {
              assertThat(persist.updateConditional(k, raceObj)).isTrue();
            } catch (ObjTooLargeException e) {
              throw new RuntimeException(e);
            }
          }
          // continue with real impl, let it race
          return null;
        };

    SignerKey key2 = service.currentSignerKey();
    soft.assertThat(key2).isNotEqualTo(key1).isEqualTo(raceKey);

    verify(service, times(1)).updateKeys(any(), any());
  }
}
