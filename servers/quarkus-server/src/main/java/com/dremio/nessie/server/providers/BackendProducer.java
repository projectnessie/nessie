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

package com.dremio.nessie.server.providers;

import java.util.Optional;

import javax.enterprise.inject.Produces;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.backend.dynamodb.DynamoDbBackend;
import com.dremio.nessie.backend.simple.InMemory;

/**
 * Factory to generate backend based on server config.
 */
@Singleton
public class BackendProducer {

  @ConfigProperty(name = "nessie.backends.type", defaultValue = "INMEMORY")
  String type;
  @ConfigProperty(name = "quarkus.dynamodb.aws.region")
  String region;
  @ConfigProperty(name = "quarkus.dynamodb.endpoint-override")
  Optional<String> endpoint;


  /**
   * produce a backend based on config.
   */
  @Singleton
  @Produces
  public Backend producer() {
    System.out.println("OH YEAH " + type);
    if (type.equals("INMEMORY")) {
      return new InMemory.BackendFactory().create(null, null);
    }
    return new DynamoDbBackend.BackendFactory().create(region, endpoint.orElse(null));
  }

}
