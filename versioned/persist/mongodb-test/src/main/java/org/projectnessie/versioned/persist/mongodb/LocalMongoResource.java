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
package org.projectnessie.versioned.persist.mongodb;

import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class LocalMongoResource implements ExtensionContext.Store.CloseableResource {

  private LocalMongo localMongo;

  private synchronized LocalMongo mongo() {
    if (localMongo == null) {
      LocalMongo mongo = new LocalMongo();
      mongo.startMongo(Optional.empty(), true);
      localMongo = mongo;
    }
    return localMongo;
  }

  public String getConnectionString() {
    return mongo().getConnectionString();
  }

  public String getDatabaseName() {
    return mongo().getDatabaseName();
  }

  @Override
  public synchronized void close() {
    if (localMongo == null) {
      return;
    }
    try {
      localMongo.stop();
    } finally {
      localMongo = null;
    }
  }
}
