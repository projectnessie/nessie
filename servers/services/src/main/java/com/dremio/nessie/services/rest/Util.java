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
package com.dremio.nessie.services.rest;

import java.security.Principal;
import java.util.NoSuchElementException;
import java.util.Optional;

import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.HttpHeaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.ImmutableCommitMeta;

final class Util {
  private static final Logger logger = LoggerFactory.getLogger(Util.class);

  private Util() {

  }

  static Optional<String> version(HttpHeaders headers) {
    try {
      String ifMatch = headers.getHeaderString(HttpHeaders.IF_MATCH);
      return Optional.of(EntityTag.valueOf(ifMatch).getValue());
    } catch (NullPointerException | NoSuchElementException | IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  static CommitMeta meta(
      Principal principal,
      String message,
      String ref) {
    return ImmutableCommitMeta.builder()
                              .commiter(name(principal))
                              .message(message)
                              .build();
  }

  static String name(Principal principal) {
    return principal == null ? "" : principal.getName();
  }


}
