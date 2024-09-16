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
package org.projectnessie.services.rest.common;

import com.google.common.base.Throwables;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.Arrays;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ErrorCodeAware;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieErrorDetails;
import org.projectnessie.model.CommitMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RestCommon {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestCommon.class);

  private RestCommon() {}

  public static NessieError buildNessieError(
      String message,
      int statusCode,
      String statusReason,
      ErrorCode errorCode,
      Exception e,
      boolean includeExceptionStackTrace,
      Function<String, String> requestHeader) {

    if (message == null) {
      message = "";
    }

    String stack = includeExceptionStackTrace ? Throwables.getStackTraceAsString(e) : null;

    LOGGER.debug(
        "Failure on server, propagated to client. Status: {} {}, Message: {}.",
        statusCode,
        statusReason,
        message,
        e);

    NessieErrorDetails errorDetails =
        (e instanceof ErrorCodeAware) ? ((ErrorCodeAware) e).getErrorDetails() : null;
    if (errorDetails != null && !isNessieClientSpec2(requestHeader)) {
      errorDetails = null;
    }

    return ImmutableNessieError.builder()
        .message(message)
        .status(statusCode)
        .errorCode(errorCode)
        .reason(statusReason)
        .serverStackTrace(stack)
        .errorDetails(errorDetails)
        .build();
  }

  private static boolean isNessieClientSpec2(Function<String, String> requestHeader) {
    String clientSpec = requestHeader.apply("Nessie-Client-Spec");
    if (clientSpec == null) {
      return false;
    }
    try {
      int i = clientSpec.indexOf('.');
      if (i != -1) {
        clientSpec = clientSpec.substring(0, i);
      }
      return Integer.parseInt(clientSpec) >= 2;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static CommitMeta.Builder updateCommitMeta(
      CommitMeta.Builder commitMeta, HttpHeaders httpHeaders) {
    httpHeaders
        .getRequestHeaders()
        .forEach(
            (k, v) -> {
              if (!v.isEmpty()) {
                String lower = k.toLowerCase(Locale.ROOT);
                switch (lower) {
                  case "nessie-commit-message":
                    v.stream()
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .findFirst()
                        .ifPresent(commitMeta::message);
                    break;
                  case "nessie-commit-authors":
                    v.stream()
                        .flatMap(s -> Arrays.stream(s.split(",")))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .forEach(commitMeta::addAllAuthors);
                    break;
                  case "nessie-commit-signedoffby":
                    v.stream()
                        .flatMap(s -> Arrays.stream(s.split(",")))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .forEach(commitMeta::addAllSignedOffBy);
                    break;
                  default:
                    if (lower.startsWith("nessie-commit-property-")) {
                      String prop = lower.substring("nessie-commit-property-".length()).trim();
                      commitMeta.putAllProperties(
                          prop,
                          v.stream()
                              .map(String::trim)
                              .filter(s -> !s.isEmpty())
                              .collect(Collectors.toList()));
                    }
                    break;
                }
              }
            });

    return commitMeta;
  }
}
