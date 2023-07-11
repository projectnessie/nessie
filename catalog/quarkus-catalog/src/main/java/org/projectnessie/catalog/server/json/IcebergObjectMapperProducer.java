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
package org.projectnessie.catalog.server.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Singleton;
import org.projectnessie.catalog.service.ee.javax.ContextObjectMapper;
import org.projectnessie.catalog.service.util.Json;

@ApplicationScoped
public class IcebergObjectMapperProducer {

  /**
   * Produces the {@link ObjectMapper} used by the Iceberg REST API.
   *
   * <p>We already produce this specific mapper through {@link ContextObjectMapper}. But
   * unfortunately, ContextResolver @Provider-annotated classes are a JAX-RS mechanism, and
   * sometimes they are not picked up by the CDI container, for example in {@link
   * io.quarkus.resteasy.reactive.jackson.runtime.serialisers.ServerJacksonMessageBodyReader}. So we
   * also produce the mapper here, to make sure it is available everywhere.
   *
   * <p>The bean produced here will override the default bean produced by {@link
   * io.quarkus.jackson.runtime.ObjectMapperProducer}.
   */
  @Singleton
  public ObjectMapper produceObjectMapper() {
    return Json.OBJECT_MAPPER;
  }
}
