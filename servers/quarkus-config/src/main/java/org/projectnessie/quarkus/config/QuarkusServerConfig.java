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

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import org.projectnessie.services.config.ServerConfig;

@ConfigMapping(prefix = "nessie.server")
public interface QuarkusServerConfig extends ServerConfig {

  @Override
  @WithName("default-branch")
  @WithDefault("main")
  String getDefaultBranch();

  @Override
  @WithName("send-stacktrace-to-client")
  @WithDefault("false")
  boolean sendStacktraceToClient();

  @Override
  @WithName("access-checks-batch-size")
  @WithDefault("" + ServerConfig.DEFAULT_ACCESS_CHECK_BATCH_SIZE)
  int accessChecksBatchSize();
}
