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
package org.projectnessie.catalog.secrets;

import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;
import static org.projectnessie.catalog.secrets.SecretJsonParser.parseOrSingle;
import static org.projectnessie.catalog.secrets.TokenSecret.tokenSecret;

import java.util.Map;

public enum SecretType {
  BASIC() {
    @Override
    public Secret fromValueMap(Map<String, String> value) {
      return basicCredentials(value);
    }
  },
  KEY() {
    @Override
    public Secret fromValueMap(Map<String, String> value) {
      return keySecret(value);
    }

    @Override
    public Secret parse(String string) {
      return keySecret(string);
    }
  },
  EXPIRING_TOKEN() {
    @Override
    public Secret fromValueMap(Map<String, String> value) {
      return tokenSecret(value);
    }
  },
  ;

  /** Construct a {@link Secret} instance from its map representation. */
  public abstract Secret fromValueMap(Map<String, String> value);

  public Secret parse(String string) {
    return fromValueMap(parseOrSingle(string));
  }
}
