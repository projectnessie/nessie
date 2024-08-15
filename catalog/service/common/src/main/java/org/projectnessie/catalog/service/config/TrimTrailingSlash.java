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
package org.projectnessie.catalog.service.config;

import static org.projectnessie.catalog.service.config.CatalogConfig.removeTrailingSlash;

import org.eclipse.microprofile.config.spi.Converter;

public class TrimTrailingSlash implements Converter<String> {
  @Override
  public String convert(String s) throws IllegalArgumentException, NullPointerException {
    if (s == null) {
      return null;
    }
    return removeTrailingSlash(s);
  }
}
