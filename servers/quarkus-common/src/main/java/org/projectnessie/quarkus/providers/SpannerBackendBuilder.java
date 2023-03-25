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
package org.projectnessie.quarkus.providers;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.SPANNER;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.quarkiverse.googlecloudservices.common.GcpBootstrapConfiguration;
import io.quarkiverse.googlecloudservices.common.GcpConfigHolder;
import io.quarkus.arc.Arc;
import io.quarkus.arc.properties.UnlessBuildProperty;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import org.projectnessie.quarkus.config.QuarkusSpannerConfig;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.spanner.SpannerBackendConfig;
import org.projectnessie.versioned.storage.spanner.SpannerBackendFactory;

@StoreType(SPANNER)
@Dependent
@UnlessBuildProperty(name = "quarkus.package.type", stringValue = "native")
public class SpannerBackendBuilder implements BackendBuilder {

  @Inject QuarkusSpannerConfig spannerConfig;

  @Override
  public Backend buildBackend() {
    GoogleCredentials googleCredentials = null;
    Exception googleCredentialsException = null;
    try {
      googleCredentials = Arc.container().instance(GoogleCredentials.class).get();
    } catch (Exception e) {
      googleCredentialsException = e;
    }
    GcpConfigHolder gcpConfigHolder = Arc.container().instance(GcpConfigHolder.class).get();

    GcpBootstrapConfiguration gcpConfiguration = gcpConfigHolder.getBootstrapConfig();
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder().setProjectId(gcpConfiguration.projectId.orElse(null));
    if (!spannerConfig.emulatorHost().isEmpty()) {
      builder.setEmulatorHost(spannerConfig.emulatorHost());
    } else {
      if (googleCredentials == null) {
        if (googleCredentialsException != null) {
          throw new RuntimeException("No GoogleCredentials available", googleCredentialsException);
        } else {
          throw new RuntimeException("No GoogleCredentials available");
        }
      }
      builder.setCredentials(googleCredentials);
    }
    Spanner spanner = builder.build().getService();

    SpannerBackendFactory factory = new SpannerBackendFactory();
    DatabaseId databaseId =
        DatabaseId.of(
            spanner.getOptions().getProjectId(),
            spannerConfig.instanceId(),
            spannerConfig.databaseId());
    SpannerBackendConfig c =
        SpannerBackendConfig.builder().spanner(spanner).databaseId(databaseId).build();
    return factory.buildBackend(c);
  }
}
