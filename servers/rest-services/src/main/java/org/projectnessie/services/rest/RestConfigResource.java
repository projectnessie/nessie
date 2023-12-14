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

import com.fasterxml.jackson.annotation.JsonView;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.projectnessie.api.v1.http.HttpConfigApi;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.ser.Views;
import org.projectnessie.services.spi.ConfigService;

/** REST endpoint to retrieve server settings. */
@RequestScoped
public class RestConfigResource implements HttpConfigApi {

  private final ConfigService configService;

  // Mandated by CDI 2.0
  public RestConfigResource() {
    this(null);
  }

  @Inject
  public RestConfigResource(ConfigService configService) {
    this.configService = configService;
  }

  @Override
  @JsonView(Views.V1.class)
  public NessieConfiguration getConfig() {
    return configService.getConfig();
  }
}
