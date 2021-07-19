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
package org.projectnessie.services.config;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;

/** This class needs to be in the same package as {@link ServerConfig}. */
public class ServerConfigExtension implements Extension {
  public static final ServerConfig SERVER_CONFIG =
      new ServerConfig() {
        @Override
        public String getDefaultBranch() {
          return "main";
        }

        @Override
        public boolean sendStacktraceToClient() {
          return false;
        }
      };

  @SuppressWarnings("unused")
  public void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm) {
    abd.addBean()
        .addType(ServerConfig.class)
        .addQualifier(Default.Literal.INSTANCE)
        .scope(ApplicationScoped.class)
        .produceWith(i -> SERVER_CONFIG);
  }
}
