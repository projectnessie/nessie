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
package org.projectnessie.services.rest;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;

import org.projectnessie.api.rest.ConfigRestApi;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.ConfigApiImpl;

/**
 * REST endpoint to retrieve server settings.
 */
@RequestScoped
public class ConfigResource implements ConfigRestApi {

  /**
   * Delegate to (do not extend) the implementation for better code-coverage.
   */
  private final ConfigApiImpl delegate;

  @Inject
  public ConfigResource(ServerConfig config) {
    delegate = new ConfigApiImpl(config);
  }

  @Override
  public NessieConfiguration getConfig() {
    return delegate.getConfig();
  }

}
