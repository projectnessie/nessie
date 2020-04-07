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

package com.dremio.iceberg.client.auth;

import com.dremio.iceberg.jwt.KeyGenerator;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import java.security.Key;

@SuppressWarnings("LineLength")
public class PublicKeyGenerator implements KeyGenerator {
  private static final String pk = "MIGbMBAGByqGSM49AgEGBSuBBAAjA4GGAAQAdqgrp7VXjhHX/NzHxqpS7u+T1z0nXAJ2maDr2+sxQtiYnAeeVpYhMO9Vl3QisDgIg8pntHWsHweOiLvtaVjHWuMAaPmI/9QXgfof9hICLMfNPxDqyDtBBacayKZ1BAEdtr30W3F0/yGY+LIZjiwgwfFbcfGMG/J1EeaIvWwYwVxR0LE=";

  @Override
  public Key generateKey() {
    return Keys.hmacShaKeyFor(Decoders.BASE64.decode(pk));
  }


}
