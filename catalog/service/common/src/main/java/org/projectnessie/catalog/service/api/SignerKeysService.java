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
package org.projectnessie.catalog.service.api;

import jakarta.annotation.Nullable;
import org.projectnessie.catalog.service.objtypes.SignerKey;

/**
 * Provides a stateless service providing named secret keys, called <em>signer keys</em>. Those
 * signer keys are for example used to sign the S3 sign endpoint parameters.
 */
public interface SignerKeysService {

  /**
   * Returns the current {@linkplain SignerKey signer key} that can be used for signing purposes,
   * the implementation ensures that the returned key will not expire within "too soon" (usually a
   * few days or many hours).
   */
  SignerKey currentSignerKey();

  /**
   * Retrieve a {@linkplain SignerKey signer key} by name, returns {@code null} if no such signer
   * key exists.
   */
  @Nullable
  SignerKey getSignerKey(String signerKey);
}
