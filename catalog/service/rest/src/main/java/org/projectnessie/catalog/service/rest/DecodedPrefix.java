/*
 * Copyright (C) 2023 Dremio
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

import org.immutables.value.Value;
import org.projectnessie.api.v2.params.ParsedReference;

/**
 * Information from the Iceberg REST {@code prefix} path parameter, which can contain a Nessie
 * reference (named reference, with or without commit ID or detached commit ID) and the warehouse
 * name.
 */
@Value.Immutable
public interface DecodedPrefix {
  @Value.Parameter(order = 1)
  ParsedReference parsedReference();

  @Value.Parameter(order = 2)
  String warehouse();

  static DecodedPrefix decodedPrefix(ParsedReference parsedReference, String warehouse) {
    return ImmutableDecodedPrefix.of(parsedReference, warehouse);
  }
}
