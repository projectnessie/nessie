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

package com.dremio.nessie.server;

import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import com.codahale.metrics.servlet.InstrumentedFilter;
import com.dremio.nessie.auth.Users;
import com.dremio.nessie.json.ObjectMapperContextResolver;
import com.dremio.nessie.server.auth.NessieAuthFilter;
import com.dremio.nessie.server.rest.Login;
import com.dremio.nessie.server.rest.ServerStatus;
import com.dremio.nessie.server.rest.TableBranchOperations;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import javax.ws.rs.ApplicationPath;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.internal.util.PropertiesHelper;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for Nessie REST service.
 */
@ApplicationPath("/api/v1")
public class RestServerV1 extends ResourceConfig {
  private static final Logger logger = LoggerFactory.getLogger(RestServerV1.class);

  public RestServerV1() {
    this(new NessieServerBinder());
  }

  /**
   * Construct a rest server given a dependency injection binder.
   */
  public RestServerV1(AbstractBinder binder) {
    super.packages("io.swagger.sample.resource", "io.swagger.v3.jaxrs2.integration.resources");
    super.register(binder);
    super.register(RolesAllowedDynamicFeature.class);
    init();
  }

  private void init() {

    // FILTERS //
    register(new InstrumentedResourceMethodApplicationListener(InstrumentationFilter.REGISTRY));
    register(InstrumentedFilter.class);

    // RESOURCES //
    register(ServerStatus.class);
    register(TableBranchOperations.class);
    ServerConfiguration config = new ServerConfigurationImpl.ConfigurationFactory().provide();
    if (config.getAuthenticationConfiguration().getEnableLoginEndpoint()) {
      register(Login.class);
    }
    if (config.getAuthenticationConfiguration().getEnableUsersEndpoint()) {
      register(Users.class);
    }

    // EXCEPTION MAPPERS //
    register(JsonParseExceptionMapper.class);
    register(JsonMappingExceptionMapper.class);

    try {
      String name = config.getAuthenticationConfiguration().getAuthFilterClassName();
      Class<?> clazz = Class.forName(name);
      register(clazz);
    } catch (ClassNotFoundException e) {
      logger.error("Auth filter class not found, unable to start");
      throw new RuntimeException(e);
    }
    register(ObjectMapperContextResolver.class);
    // PROPERTIES //
    property(ServerProperties.RESPONSE_SET_STATUS_OVER_SEND_ERROR, "true");

    final String disableMoxy = PropertiesHelper.getPropertyNameForRuntime(
        CommonProperties.MOXY_JSON_FEATURE_DISABLE,
        getConfiguration().getRuntimeType());
    property(disableMoxy, true);
  }


}
