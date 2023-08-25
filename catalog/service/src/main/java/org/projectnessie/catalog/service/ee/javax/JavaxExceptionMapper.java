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
package org.projectnessie.catalog.service.ee.javax;

import java.util.List;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.projectnessie.catalog.api.errors.GenericIcebergRestException;
import org.projectnessie.catalog.api.errors.OAuthTokenEndpointException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ContentKeyErrorDetails;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieErrorDetails;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieRuntimeException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.ContentKey;

@Provider
public class JavaxExceptionMapper implements ExceptionMapper<Exception> {

  @Override
  public Response toResponse(Exception ex) {
    if (ex instanceof BaseNessieClientServerException) {
      BaseNessieClientServerException e = (BaseNessieClientServerException) ex;
      ErrorCode err = e.getErrorCode();
      return mapNessieErrorCodeException(e, err, e.getErrorDetails());
    }
    if (ex instanceof NessieRuntimeException) {
      NessieRuntimeException e = (NessieRuntimeException) ex;
      ErrorCode err = e.getErrorCode();
      return mapNessieErrorCodeException(e, err, null);
    }
    if (ex instanceof GenericIcebergRestException) {
      GenericIcebergRestException c = (GenericIcebergRestException) ex;
      return genericErrorResponse(c.getResponseCode(), c.getType(), c.getMessage());
    }
    if (ex instanceof OAuthTokenEndpointException) {
      OAuthTokenEndpointException e = (OAuthTokenEndpointException) ex;
      return authEndpointErrorResponse(e);
    }
    if (ex instanceof IllegalArgumentException) {
      return genericErrorResponse(400, ex.getClass().getSimpleName(), ex.getMessage());
    }
    if (ex instanceof WebApplicationException) {
      return ((WebApplicationException) ex).getResponse();
    }
    return serverErrorException(ex);
  }

  private static Response mapNessieErrorCodeException(
      Exception ex, ErrorCode err, NessieErrorDetails errorDetails) {
    switch (err) {
      case UNSUPPORTED_MEDIA_TYPE:
      case BAD_REQUEST:
        return genericErrorResponse(400, "BadRequestException", ex.getMessage());
      case FORBIDDEN:
        return genericErrorResponse(403, "NotAuthorizedException", ex.getMessage());
      case CONTENT_NOT_FOUND:
        return genericErrorResponse(
            404, "NoSuchTableException", "Table does not exist: " + keyMessage(ex, errorDetails));
      case NAMESPACE_ALREADY_EXISTS:
        return genericErrorResponse(
            409,
            "AlreadyExistsException",
            "Namespace already exists: " + keyMessage(ex, errorDetails));
      case NAMESPACE_NOT_EMPTY:
        return genericErrorResponse(409, "", ex.getMessage());
      case NAMESPACE_NOT_FOUND:
        return genericErrorResponse(
            404,
            "NoSuchNamespaceException",
            "Namespace does not exist: " + keyMessage(ex, errorDetails));
      case REFERENCE_ALREADY_EXISTS:
        return genericErrorResponse(409, "", ex.getMessage());
      case REFERENCE_CONFLICT:
        if (ex instanceof NessieReferenceConflictException) {
          NessieReferenceConflictException referenceConflictException =
              (NessieReferenceConflictException) ex;
          ReferenceConflicts referenceConflicts = referenceConflictException.getErrorDetails();
          if (referenceConflicts != null) {
            List<Conflict> conflicts = referenceConflicts.conflicts();
            if (conflicts.size() == 1) {
              Conflict conflict = conflicts.get(0);
              ConflictType conflictType = conflict.conflictType();
              if (conflictType != null) {
                switch (conflictType) {
                  case NAMESPACE_ABSENT:
                    return genericErrorResponse(
                        404,
                        "NoSuchNamespaceException",
                        "Namespace does not exist: " + conflict.key());
                  case KEY_DOES_NOT_EXIST:
                    return genericErrorResponse(
                        404, "NoSuchTableException", "Table does not exist: " + ex.getMessage());
                  default:
                    break;
                }
              }
            }
          }
        }
        return genericErrorResponse(409, "", ex.getMessage());
      case REFERENCE_NOT_FOUND:
        return genericErrorResponse(400, "NoSuchReferenceException", ex.getMessage());
      case UNKNOWN:
      case REFLOG_NOT_FOUND:
      case TOO_MANY_REQUESTS:
      default:
        break;
    }
    return serverErrorException(ex);
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

  private static Response serverErrorException(Exception ex) {
    return serverErrorException(ex.getClass().getSimpleName(), ex.getMessage());
  }

  private static Response serverErrorException(String type, String message) {
    return genericErrorResponse(500, type, message);
  }

  private static Response genericErrorResponse(int code, String type, String message) {
    return Response.status(code)
        .entity(
            ErrorResponse.builder().responseCode(code).withType(type).withMessage(message).build())
        .build();
  }

  private static Response authEndpointErrorResponse(OAuthTokenEndpointException e) {
    return Response.status(e.getCode()).entity(e.getDetails()).build();
  }
}
