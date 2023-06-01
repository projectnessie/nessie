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
package org.projectnessie.quarkus.providers;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;
import javax.inject.Singleton;
import org.projectnessie.quarkus.config.QuarkusVersionStoreAdvancedConfig;
import org.projectnessie.versioned.storage.common.config.StoreConfig;

@ApplicationScoped
public class RepositoryIdProvider {

  /** The name of the CDI bean that provides the repository ID. */
  public static final String REPOSITORY_ID_BEAN_NAME = "nessie.beans.repository-id";

  /**
   * Produces a bean containing the (system-wide) repository id.
   *
   * <p>A bean named {@value #REPOSITORY_ID_BEAN_NAME} of type String is required by Nessie events
   * notification system.
   *
   * <p>Currently, the bean is a singleton produced from Nessie's configuration, but this could
   * change in the future to be a request-scoped bean, if repository-based multi-tenancy is
   * introduced one day.
   */
  @Produces
  @Singleton
  @Named(REPOSITORY_ID_BEAN_NAME)
  public String produceRepositoryId(
      StoreConfig config, QuarkusVersionStoreAdvancedConfig legacyConfig) {
    String repositoryId = config.repositoryId();
    if (repositoryId.isEmpty()) {
      repositoryId = legacyConfig.getRepositoryId();
    }
    return repositoryId;
  }
}
