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
package org.projectnessie.versioned.storage.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.immutables.value.Value;

@Value.Immutable
public abstract class MongoClientProducer {

  static ImmutableMongoClientProducer.Builder builder() {
    return ImmutableMongoClientProducer.builder();
  }

  abstract String connectionString();

  public MongoClient createClient() {
    return MongoClients.create(connectionString());
  }
}
