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
package com.dremio.iceberg.server;

import javax.ws.rs.ApplicationPath;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.internal.util.PropertiesHelper;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;

import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import com.codahale.metrics.servlet.InstrumentedFilter;
import com.dremio.iceberg.server.auth.AlleyAuthFilter;
import com.dremio.iceberg.server.rest.ListTables;
import com.dremio.iceberg.server.rest.ListTags;
import com.dremio.iceberg.server.rest.Login;
import com.dremio.iceberg.server.rest.ServerStatus;
import com.dremio.iceberg.auth.Users;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;

@ApplicationPath("/api/v1")
public class RestServerV1 extends ResourceConfig {

  public RestServerV1() {
    this(new AlleyServerBinder());
  }

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
    ServerConfiguration config = new ServerConfigurationImpl.ConfigurationFactory().provide();
    register(ListTables.class);
    register(ListTags.class);
    register(ServerStatus.class);
    if (config.getAuthenticationConfiguration().loginEndpointEnabled()) {
      register(Login.class);
    }
    if (config.getAuthenticationConfiguration().userEndpointEnabled()) {
      register(Users.class);
    }

    // EXCEPTION MAPPERS //
    register(JsonParseExceptionMapper.class);
    register(JsonMappingExceptionMapper.class);


    register(AlleyAuthFilter.class);
    register(ObjectMapperContextResolver.class);
    // PROPERTIES //
    property(ServerProperties.RESPONSE_SET_STATUS_OVER_SEND_ERROR, "true");

    final String disableMoxy = PropertiesHelper.getPropertyNameForRuntime(CommonProperties.MOXY_JSON_FEATURE_DISABLE,
        getConfiguration().getRuntimeType());
    property(disableMoxy, true);
  }


}
