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
package org.projectnessie.server.providers;

import java.util.function.Consumer;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.projectnessie.error.NessieError;
import org.projectnessie.services.config.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import io.quarkus.arc.Arc;
import io.quarkus.arc.InjectableInstance;

/**
 * Code shared between concrete exception-mapper implementations.
 */
abstract class BaseExceptionMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseExceptionMapper.class);

  protected BaseExceptionMapper() {
    // empty
  }

  Response buildExceptionResponse(
      int status,
      String reason,
      String message,
      Exception e) {

    // Must not use `@Inject ServerConfig serverConfig`, because once that's being used
    // for our exception-mappers, it will actually not be injected and the field will be `null`.
    // That only happens, if there is a `implements ExceptionMapper<ValidationException>`
    // bean, probably a bean that uses some infra/library-stuff and then the DI mechanism
    // fails to inject it.
    InjectableInstance<ServerConfig> injectableInstance = Arc.container()
        .select(ServerConfig.class);
    ServerConfig serverConfig = injectableInstance.get();

    return buildExceptionResponse(
        status,
        reason,
        message,
        e,
        serverConfig.sendStacktraceToClient(),
        h -> {});
  }

  Response buildExceptionResponse(
      int status,
      String reason,
      String message,
      Exception e,
      boolean includeExceptionStackTrace,
      Consumer<ResponseBuilder> responseHandler) {

    String stack = includeExceptionStackTrace ? Throwables.getStackTraceAsString(e) : null;
    NessieError error = new NessieError(message, status, reason, stack);
    LOGGER.debug("Failure on server, propagated to client. Status: {} {}, Message: {}.",
        status, reason, message, e);
    ResponseBuilder responseBuilder = Response.status(status)
        .entity(error)
        .type(MediaType.APPLICATION_JSON_TYPE);
    responseHandler.accept(responseBuilder);
    return responseBuilder.build();
  }
}
