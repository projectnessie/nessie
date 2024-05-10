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
package org.projectnessie.catalog.service.rest;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.typeToEntityName;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergError.icebergError;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergErrorResponse.icebergErrorResponse;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergErrorResponse;
import org.projectnessie.catalog.service.api.CatalogEntityAlreadyExistsException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ContentKeyErrorDetails;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieErrorDetails;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;
import org.projectnessie.services.config.ExceptionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception mapper for the Iceberg REST API. This mapper can handle Nessie Core exceptions, but
 * converts them to HTTP payloads appropriate for Iceberg REST clients as opposed to Nessie API
 * responses.
 */
@Singleton
public class IcebergErrorMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergErrorMapper.class);

  private final ExceptionConfig exceptionConfig;

  @Inject
  public IcebergErrorMapper(ExceptionConfig exceptionConfig) {
    this.exceptionConfig = exceptionConfig;
  }

  public Response toResponse(Throwable ex, IcebergEntityKind kind) {
    if (ex instanceof CompletionException) {
      ex = ex.getCause();
    }

    if (ex.getClass() == RuntimeException.class && ex.getCause() != null) {
      ex = ex.getCause();
    }

    IcebergErrorResponse body = null;
    if (ex instanceof BaseNessieClientServerException) {
      BaseNessieClientServerException e = (BaseNessieClientServerException) ex;
      body = mapNessieError(e, e.getErrorCode(), e.getErrorDetails(), kind);
    } else if (ex instanceof IllegalArgumentException) {
      body = errorResponse(400, "IllegalArgumentException", ex.getMessage(), ex);
    }

    if (body == null) {
      LOGGER.warn("Unhandled exception returned as HTTP/500: {}", ex, ex);
      body = errorResponse(500, ex.getClass().getSimpleName(), ex.getMessage(), ex);
    }

    Integer code = body.error().code();
    return Response.status(code == null ? 500 : code).entity(body).build();
  }

  private IcebergErrorResponse mapNessieError(
      Exception ex, ErrorCode err, NessieErrorDetails errorDetails, IcebergEntityKind kind) {
    switch (err) {
      case UNSUPPORTED_MEDIA_TYPE:
      case BAD_REQUEST:
        return errorResponse(400, "BadRequestException", ex.getMessage(), ex);
      case FORBIDDEN:
        return errorResponse(403, "NotAuthorizedException", ex.getMessage(), ex);
      case CONTENT_NOT_FOUND:
        return errorResponse(
            404,
            kind.notFoundExceptionName(),
            kind.entityName() + " does not exist: " + keyMessage(ex, errorDetails),
            ex);
      case NAMESPACE_ALREADY_EXISTS:
        return errorResponse(
            409,
            "AlreadyExistsException",
            "Namespace already exists: " + keyMessage(ex, errorDetails),
            ex);
      case NAMESPACE_NOT_EMPTY:
      case REFERENCE_ALREADY_EXISTS:
        return errorResponse(409, "", ex.getMessage(), ex);
      case NAMESPACE_NOT_FOUND:
        return errorResponse(
            404,
            "NoSuchNamespaceException",
            "Namespace does not exist: " + keyMessage(ex, errorDetails),
            ex);
      case REFERENCE_CONFLICT:
        if (ex instanceof NessieReferenceConflictException) {
          NessieReferenceConflictException referenceConflictException =
              (NessieReferenceConflictException) ex;
          ReferenceConflicts referenceConflicts = referenceConflictException.getErrorDetails();
          if (referenceConflicts != null) {
            List<Conflict> conflicts = referenceConflicts.conflicts();
            if (conflicts.size() == 1) {
              Conflict conflict = conflicts.get(0);
              IcebergErrorResponse mapped = mapConflict(conflict, referenceConflictException);
              if (mapped != null) {
                return mapped;
              }
            }
          }
        }
        return errorResponse(409, "", ex.getMessage(), ex);
      case REFERENCE_NOT_FOUND:
        return errorResponse(400, "NoSuchReferenceException", ex.getMessage(), ex);
      case UNKNOWN:
      case TOO_MANY_REQUESTS:
      default:
        break;
    }

    return null;
  }

  private IcebergErrorResponse errorResponse(int code, String type, String message, Throwable ex) {
    List<String> stack;
    if (exceptionConfig.sendStacktraceToClient()) {
      stack =
          Arrays.stream(ex.getStackTrace())
              .map(StackTraceElement::toString)
              .collect(Collectors.toList());
    } else {
      stack = emptyList();
    }

    return icebergErrorResponse(icebergError(code, type, message, stack));
  }

  private static String keyMessage(Exception ex, NessieErrorDetails errorDetails) {
    if (errorDetails instanceof ContentKeyErrorDetails) {
      ContentKey key = ((ContentKeyErrorDetails) errorDetails).contentKey();
      if (key != null) {
        return key.toString();
      }
    }
    return ex.getMessage();
  }

  private IcebergErrorResponse mapConflict(Conflict conflict, NessieReferenceConflictException ex) {
    Conflict.ConflictType conflictType = conflict.conflictType();
    switch (conflictType) {
      case NAMESPACE_ABSENT:
        return errorResponse(
            404, "NoSuchNamespaceException", "Namespace does not exist: " + conflict.key(), ex);

      case KEY_EXISTS:
        if (ex instanceof CatalogEntityAlreadyExistsException) {
          CatalogEntityAlreadyExistsException e = (CatalogEntityAlreadyExistsException) ex;
          // Produces different messages depending on the target type - just to get the tests
          // passing :facepalm:
          String type = typeToEntityName(e.getExistingType());
          String msg;
          if (e.isRename()) {
            msg =
                format(
                    "Cannot rename %s to %s. %s already exists",
                    e.getConflictingKey(), e.getExistingKey(), type);
          } else {
            String prefix = "";
            if (e.isRequirement() && e.getExistingType().equals(ICEBERG_TABLE)) {
              prefix = "Requirement failed: ";
              type = type.toLowerCase(Locale.ROOT);
            }

            msg =
                format(
                    "%s%s %salready exists: %s",
                    prefix,
                    type,
                    e.getConflictingType().equals(e.getExistingType()) ? "" : "with same name ",
                    e.getExistingKey());
          }

          return errorResponse(409, "AlreadyExistsException", msg, ex);
        }
        return null;

      default:
        return null;
    }
  }

  public enum IcebergEntityKind {
    UNKNOWN("UnknownEntity", "NotFoundException"),
    NAMESPACE("Namespace", "NoSuchNamespaceException"),
    TABLE("Table", "NoSuchTableException"),
    VIEW("View", "NoSuchViewException");

    private final String name;
    private final String icebergNotFoundExceptionName;

    IcebergEntityKind(String name, String icebergNotFoundExceptionName) {
      this.name = name;
      this.icebergNotFoundExceptionName = icebergNotFoundExceptionName;
    }

    private String entityName() {
      return name;
    }

    private String notFoundExceptionName() {
      return icebergNotFoundExceptionName;
    }
  }
}
