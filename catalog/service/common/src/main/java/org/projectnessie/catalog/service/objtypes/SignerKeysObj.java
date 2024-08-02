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
package org.projectnessie.catalog.service.objtypes;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.projectnessie.versioned.storage.common.objtypes.CustomObjType.customObjType;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@NessieImmutable
@JsonSerialize(as = ImmutableSignerKeysObj.class)
@JsonDeserialize(as = ImmutableSignerKeysObj.class)
// Suppress: "Constructor parameters should be better defined on the same level of inheritance
// hierarchy..."
@SuppressWarnings("immutables:subtype")
public interface SignerKeysObj extends UpdateableObj {
  ObjType OBJ_TYPE = customObjType("signer-keys", "sig-keys", SignerKeysObj.class);

  ObjId OBJ_ID = ObjId.objIdFromByteArray("signer-keys-singleton".getBytes(UTF_8));

  static ImmutableSignerKeysObj.Builder builder() {
    return ImmutableSignerKeysObj.builder();
  }

  @Override
  @Value.Default
  default ObjId id() {
    return OBJ_ID;
  }

  @Override
  @Value.Default
  default ObjType type() {
    return OBJ_TYPE;
  }

  /**
   * Signer keys in chronological order, most recent one at the end.
   *
   * <p>Implementation note: the whole {@link SignerKeysObj} object is deserialized from the persist
   * cache for every access, so keeping a map does not help performance.
   */
  List<SignerKey> signerKeys();

  @Value.NonAttribute
  default SignerKey getSignerKey(String name) {
    for (SignerKey signerKey : signerKeys()) {
      if (signerKey.name().equals(name)) {
        return signerKey;
      }
    }
    return null;
  }

  @Value.Check
  default void check() {
    checkState(!signerKeys().isEmpty(), "At least one signer-key is required");
    checkState(
        signerKeys().stream().map(SignerKey::name).distinct().count() == signerKeys().size(),
        "Duplicate signer key names");
  }
}
