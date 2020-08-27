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
package com.dremio.nessie.server.auth;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.util.Base64;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.crypto.spec.SecretKeySpec;

import com.dremio.nessie.jwt.KeyGenerator;
import com.google.common.base.Throwables;

/**
 * Read key from Docker secret stored at /run/secrets/nessie_key.
 * <p>
 *   If read fails, fall back to standard key
 * </p>
 */
public class SecretKeyGenerator implements KeyGenerator {

  private static final Key KEY;

  static {
    KEY = readKey();
  }

  private static Key readKey() {
    String keyPath = System.getenv().getOrDefault("NESSIE_JWT_KEY_FILE", "/run/secrets/nessie_key");
    try (Stream<String> stream = Files.lines(Paths.get(keyPath), StandardCharsets.UTF_8)) {
      String keyStr = stream.collect(Collectors.joining());
      byte[] decodedKey = Base64.getDecoder().decode(keyStr);
      return new SecretKeySpec(decodedKey, 0, decodedKey.length, "HmacSHA512");
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException("Unable to read " + keyPath, e);
    }
  }

  @Override
  public Key generateKey() {
    return KEY;
  }

}
