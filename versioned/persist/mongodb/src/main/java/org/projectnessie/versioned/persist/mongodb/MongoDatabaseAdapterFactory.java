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
package org.projectnessie.versioned.persist.mongodb;

import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.events.AdapterEventConsumer;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterFactory;

public class MongoDatabaseAdapterFactory
    extends NonTransactionalDatabaseAdapterFactory<MongoDatabaseAdapter, MongoDatabaseClient> {

  public static final String NAME = "MongoDB";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  protected MongoDatabaseAdapter create(
      NonTransactionalDatabaseAdapterConfig config,
      MongoDatabaseClient client,
      StoreWorker storeWorker,
      AdapterEventConsumer eventConsumer) {
    return new MongoDatabaseAdapter(config, client, storeWorker, eventConsumer);
  }
}
