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

package com.dremio.iceberg.jwt;

import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Encoders;
import io.jsonwebtoken.security.Keys;
import java.security.KeyPair;

public class GenerateKeys {

  public static void main(String[] args) {
    KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.ES512);
    String privateKey = Encoders.BASE64.encode(keyPair.getPrivate().getEncoded());
    String publicKey = Encoders.BASE64.encode(keyPair.getPublic().getEncoded());
    System.out.println(privateKey);
    System.out.println(publicKey);
  }

}
