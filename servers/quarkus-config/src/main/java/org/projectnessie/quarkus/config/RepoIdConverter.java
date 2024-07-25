/*
 * Copyright (C) 2022 Dremio
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

import org.eclipse.microprofile.config.spi.Converter;

/**
 * Functionality equivalent to {@link io.quarkus.runtime.configuration.TrimmedStringConverter} in
 * Quarkus 2.6.x, but the behavior of that class has changed in Quarkus 2.7.
 */
public class RepoIdConverter implements Converter<String> {

  public RepoIdConverter() {}

  @Override
  public String convert(String s) {
    if (s == null) {
      return null;
    }
    return s.trim();
  }
}
