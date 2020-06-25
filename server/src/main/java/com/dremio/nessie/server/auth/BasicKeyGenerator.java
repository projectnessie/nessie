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

import com.dremio.nessie.jwt.KeyGenerator;
import java.security.Key;
import java.util.Base64;
import javax.crypto.spec.SecretKeySpec;

/**
 * key generator for testing purposes only.
 */
@SuppressWarnings("LineLength")
public class BasicKeyGenerator implements KeyGenerator {
  private static final String KEY_STR = "6pRjiKcViKgqwDi7yhDjPa4Wmdu8vviN2na19uAfm3+1SXuAeYkT44XWlfYztK/Vo1I/gNrNKdZc62j1MvoMyw==";
  private static Key KEY = new SecretKeySpec(Base64.getDecoder().decode(KEY_STR.getBytes()), "HmacSHA256");

  @Override
  public Key generateKey() {
    return KEY;
  }
}
