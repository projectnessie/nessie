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
package org.projectnessie.catalog.service.rest;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.EnumSet;
import java.util.Locale;
import java.util.function.Predicate;

public enum AccessDelegation {
  VENDED_CREDENTIALS("vended-credentials"),
  REMOTE_SIGNING("remote-signing"),
  ;
  private final String headerValue;

  AccessDelegation(String headerValue) {
    this.headerValue = headerValue;
  }

  public String headerValue() {
    return headerValue;
  }

  /**
   * Parses the value of the {@code X-Iceberg-Access-Delegation} Iceberg REST request header.
   *
   * <p>Only recognized values of the header will be returned.
   *
   * @param headerValue header value
   * @return a {@link Predicate} used to test whether a {@link AccessDelegation} value is supported
   *     by the client. The returned {@link Predicate} always returns {@code true}, if {@code
   *     headerValue} is {@code null}, which means we don't know what the client accepts.
   */
  @Nonnull
  public static Predicate<AccessDelegation> accessDelegationPredicate(
      @Nullable String headerValue) {
    if (headerValue == null) {
      return x -> true;
    }

    EnumSet<AccessDelegation> values = EnumSet.noneOf(AccessDelegation.class);

    for (String v : headerValue.split(",")) {
      v = v.trim().toLowerCase(Locale.ROOT);
      for (AccessDelegation value : AccessDelegation.values()) {
        if (value.headerValue().equals(v)) {
          values.add(value);
        }
      }
    }

    return values::contains;
  }
}
