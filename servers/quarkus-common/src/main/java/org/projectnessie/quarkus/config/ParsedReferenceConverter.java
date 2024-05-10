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
package org.projectnessie.quarkus.config;

import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;

import org.eclipse.microprofile.config.spi.Converter;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.model.Reference;

public class ParsedReferenceConverter implements Converter<ParsedReference> {
  @Override
  public ParsedReference convert(String value)
      throws IllegalArgumentException, NullPointerException {
    try {
      // TODO inject the right default branch here
      return resolveReferencePathElement(value, Reference.ReferenceType.BRANCH, () -> "main");
    } catch (IllegalStateException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
}
