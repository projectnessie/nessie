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
import java.util.stream.Stream;

import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.jwt.KeyGenerator;

/**
 * Read key from Docker secret stored at /run/secrets/nessie_key.
 * <p>
 *   If read fails, fall back to standard key
 * </p>
 */
public class SecretKeyGenerator implements KeyGenerator {

  private static final Logger logger = LoggerFactory.getLogger(SecretKeyGenerator.class);

  private static final Key KEY;

  static {
    StringBuilder contentBuilder = new StringBuilder();
    Key key = new BasicKeyGenerator().generateKey();
    String keyPath = System.getenv().getOrDefault("NESSIE_JWT_KEY_FILE", "/run/secrets/nessie_key");
    try (Stream<String> stream = Files.lines(Paths.get(keyPath), StandardCharsets.US_ASCII)) {
      stream.forEach(contentBuilder::append);
      String keyStr = contentBuilder.toString();
      byte[] decodedKey = Base64.getDecoder().decode(keyStr);
      key = new SecretKeySpec(decodedKey, 0, decodedKey.length, "HmacSHA512");
    } catch (Exception e) {
      logger.warn("Unable to find secret file, falling back to basic key generator", e);
    }
    KEY = key;
  }

  @Override
  public Key generateKey() {
    return KEY;
  }

}
