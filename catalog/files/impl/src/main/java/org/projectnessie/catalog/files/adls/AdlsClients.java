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
package org.projectnessie.catalog.files.adls;

import com.azure.core.http.HttpClient;
import com.azure.core.util.ConfigurationBuilder;
import com.azure.core.util.HttpClientOptions;
import org.projectnessie.catalog.files.config.AdlsConfig;

public final class AdlsClients {

  private AdlsClients() {}

  public static HttpClient buildSharedHttpClient(AdlsConfig adlsConfig) {
    ConfigurationBuilder httpConfig = new ConfigurationBuilder();
    adlsConfig.configuration().forEach(httpConfig::putProperty);

    HttpClientOptions httpOptions = new HttpClientOptions().setConfiguration(httpConfig.build());
    adlsConfig.connectTimeout().ifPresent(httpOptions::setConnectTimeout);
    adlsConfig.writeTimeout().ifPresent(httpOptions::setWriteTimeout);
    adlsConfig.readTimeout().ifPresent(httpOptions::setReadTimeout);
    adlsConfig.maxHttpConnections().ifPresent(httpOptions::setMaximumConnectionPoolSize);
    adlsConfig.connectionIdleTimeout().ifPresent(httpOptions::setConnectionIdleTimeout);

    return HttpClient.createDefault(httpOptions);
  }
}
