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

import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.UUID.randomUUID;

import com.google.common.annotations.VisibleForTesting;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.projectnessie.catalog.service.api.SignerKeysService;
import org.projectnessie.catalog.service.objtypes.ImmutableSignerKeysObj;
import org.projectnessie.catalog.service.objtypes.SignerKey;
import org.projectnessie.catalog.service.objtypes.SignerKeysObj;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.Persist;

/**
 * Signer keys service implementation based on {@link Persist} using a {@link SignerKeysObj} per
 * Nessie repository. Relies on Nessie's distributed cache invalidation, when used in a horizontally
 * scaled setup (multiple Nessie processes serving the same repository).
 */
@SuppressWarnings("CdiInjectionPointsInspection")
@RequestScoped
public class SignerKeysServiceImpl implements SignerKeysService {
  public static final long SPIN_LOOP_MIN_SLEEP_MILLIS = 20;
  public static final long SPIN_LOOP_MAX_SLEEP_MILLIS = 200;
  public static final Duration NEW_KEY_ROTATE_AFTER = Duration.of(3, DAYS);
  public static final Duration NEW_KEY_EXPIRE_AFTER = Duration.of(5, DAYS);

  @Inject Persist persist;

  @VisibleForTesting Clock clock = Clock.systemUTC();

  /**
   * Called for every access to this request-scoped service implementation. Fetches the {@link
   * SignerKeysObj} or creates a new one with a signer key.
   */
  private SignerKeysObj loadOrCreate() {
    SignerKeysObj signerKeys;

    while (true) {
      try {
        signerKeys =
            persist.fetchTypedObj(
                SignerKeysObj.OBJ_ID, SignerKeysObj.OBJ_TYPE, SignerKeysObj.class);
        break;
      } catch (ObjNotFoundException notFound) {
        signerKeys = addNewKey(null);
        try {
          if (storeInitial(signerKeys)) {
            break;
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    return signerKeys;
  }

  /**
   * Updates {@code current} by adding a new {@link SignerKey} to the end of the {@linkplain
   * SignerKeysObj#signerKeys() signer-keys list} and setting a new {@linkplain
   * UpdateableObj#versionToken() version token}.
   */
  private SignerKeysObj addNewKey(SignerKeysObj current) {
    Instant now = clock.instant();
    Instant rotation = now.plus(NEW_KEY_ROTATE_AFTER);
    Instant expires = now.plus(NEW_KEY_EXPIRE_AFTER);

    byte[] secretBytes = new byte[32]; // 256 bits for SHA256
    // Use `/dev/urandom`, which does not block.
    // SecureRandom.getInstanceStrong() would use `/dev/random`, which can block, with is especially
    // an issue in containerized environments.
    new SecureRandom().nextBytes(secretBytes);

    ImmutableSignerKeysObj.Builder keys = ImmutableSignerKeysObj.builder();
    if (current != null) {
      keys.from(current);
    }

    keys.versionToken(randomUUID().toString()).signerKeys(List.of());

    if (current != null) {
      for (SignerKey signerKey : current.signerKeys()) {
        if (signerKey.expirationTime().compareTo(now) > 0) {
          keys.addSignerKey(signerKey);
        }
      }
    }

    return keys.addSignerKey(
            SignerKey.builder()
                .name(randomUUID().toString())
                .secretKey(secretBytes)
                .creationTime(now)
                .rotationTime(rotation)
                .expirationTime(expires)
                .build())
        .build();
  }

  @Override
  public SignerKey getSignerKey(String signerKey) {
    SignerKeysObj keys = loadOrCreate();
    return keys.getSignerKey(signerKey);
  }

  @Override
  public SignerKey currentSignerKey() {
    Instant now = clock.instant();

    while (true) {
      SignerKeysObj keys = loadOrCreate();

      SignerKey current = keys.signerKeys().get(keys.signerKeys().size() - 1);
      if (current.rotationTime().compareTo(now) <= 0) {
        SignerKeysObj updated = addNewKey(keys);
        try {
          if (!updateKeys(keys, updated)) {
            persistSpinLoop();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        continue;
      }

      return current;
    }
  }

  @VisibleForTesting
  boolean storeInitial(SignerKeysObj signerKeys) throws ObjTooLargeException {
    return persist.storeObj(signerKeys);
  }

  @VisibleForTesting
  boolean updateKeys(SignerKeysObj keys, SignerKeysObj updated) throws ObjTooLargeException {
    return persist.updateConditional(keys, updated);
  }

  private static void persistSpinLoop() throws InterruptedException {
    Thread.sleep(
        ThreadLocalRandom.current()
            .nextLong(SPIN_LOOP_MIN_SLEEP_MILLIS, SPIN_LOOP_MAX_SLEEP_MILLIS));
  }
}
