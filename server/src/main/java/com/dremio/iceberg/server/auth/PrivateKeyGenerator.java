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

package com.dremio.iceberg.server.auth;

import com.dremio.iceberg.jwt.KeyGenerator;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import java.security.Key;

@SuppressWarnings("LineLength")
public class PrivateKeyGenerator implements KeyGenerator {
  private static final String pk = "MGACAQAwEAYHKoZIzj0CAQYFK4EEACMESTBHAgEBBEIBOtXPpeWTMQBTPHQt10fDbMbmf1bZD/3E1sEXYdcpxagvCfMadHuWroZ5KfFL9Es2U0v31K6BnGnhtJfJ1QiYqxk=";

  @Override
  public Key generateKey() {
    byte[] bytes = Decoders.BASE64.decode(pk);
    return Keys.hmacShaKeyFor(bytes);
  }
}
