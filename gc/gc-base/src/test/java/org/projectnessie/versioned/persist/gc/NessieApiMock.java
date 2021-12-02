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
package org.projectnessie.versioned.persist.gc;

import static org.projectnessie.versioned.persist.gc.Dataset.DEFAULT_BRANCH;

import org.projectnessie.api.http.HttpContentApi;
import org.projectnessie.api.http.HttpTreeApi;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.client.http.v1api.HttpApiV1;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.services.authz.AccessCheckerExtension;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.rest.RestContentResource;
import org.projectnessie.services.rest.RestRefLogResource;
import org.projectnessie.services.rest.RestTreeResource;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.store.PersistVersionStore;

public class NessieApiMock {

  private final ServerConfig serverConfig;
  private final PersistVersionStore<Content, CommitMeta, Content.Type> versionStore;

  public NessieApiMock(DatabaseAdapter databaseAdapter) {
    versionStore = new PersistVersionStore<>(databaseAdapter, Dataset.STORE_WORKER);
    serverConfig = getServerConfig();
  }

  private ServerConfig getServerConfig() {
    return new ServerConfig() {
      @Override
      public String getDefaultBranch() {
        return DEFAULT_BRANCH;
      }

      @Override
      public boolean sendStacktraceToClient() {
        return false;
      }
    };
  }

  public HttpApiV1 getApi() {
    HttpTreeApi treeApi =
        new RestTreeResource(serverConfig, versionStore, AccessCheckerExtension.ACCESS_CHECKER);
    HttpContentApi contentApi =
        new RestContentResource(serverConfig, versionStore, AccessCheckerExtension.ACCESS_CHECKER);
    RestRefLogResource refLogApi =
        new RestRefLogResource(serverConfig, versionStore, AccessCheckerExtension.ACCESS_CHECKER);
    return new HttpApiV1(new NessieApiClient(null, treeApi, contentApi, null, refLogApi));
  }
}
