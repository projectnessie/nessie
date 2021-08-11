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
package org.projectnessie.client.grpc;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Throwables;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.security.AccessControlException;
import java.util.concurrent.Callable;
import javax.ws.rs.WebApplicationException;
import org.jboss.resteasy.api.validation.ResteasyViolationException;
import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieNotFoundException;

/** Maps gRPC exceptions to Nessie-specific exceptions and the other way around. */
public class GrpcExceptionMapper {

  /**
   * Takes the given Exception and converts it to a {@link StatusRuntimeException} with a specific
   * gRPC Status code.
   *
   * @param ex The exception to convert to a {@link StatusRuntimeException} with a specific gRPC
   *     Status code.
   * @return A new {@link StatusRuntimeException} with a specific gRPC status code.
   */
  public static StatusRuntimeException toProto(Exception ex) {
    if (ex instanceof NessieNotFoundException) {
      return Status.NOT_FOUND.withDescription(ex.getMessage()).withCause(ex).asRuntimeException();
    }

    if (ex instanceof NessieConflictException) {
      return Status.ALREADY_EXISTS
          .withDescription(ex.getMessage())
          .withCause(ex)
          .asRuntimeException();
    }

    if (ex instanceof JsonParseException
        || ex instanceof JsonMappingException
        || ex instanceof IllegalArgumentException
        || ex instanceof WebApplicationException) {
      return Status.INVALID_ARGUMENT
          .withDescription(ex.getMessage())
          .withCause(ex)
          .asRuntimeException();
    }
    if (ex instanceof ResteasyViolationException) {
      ResteasyViolationException reve = (ResteasyViolationException) ex;
      boolean returnValueViolation = !reve.getReturnValueViolations().isEmpty();
      return returnValueViolation
          ? Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException()
          : Status.INVALID_ARGUMENT
              .withDescription(ex.getMessage())
              .withCause(ex)
              .asRuntimeException();
    }
    if (ex instanceof AccessControlException) {
      return Status.PERMISSION_DENIED
          .withDescription(ex.getMessage())
          .withCause(ex)
          .asRuntimeException();
    }
    return Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException();
  }

  /**
   * Executes the given callable and reports the result to the given stream observer. Also performs
   * additional exception handling/conversion.
   *
   * @param callable The callable to call
   * @param <T> The type of the callable.
   * @param observer The {@link StreamObserver} where results are reported back to.
   */
  public static <T> void handle(Callable<T> callable, StreamObserver<T> observer) {
    try {
      observer.onNext(callable.call());
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  /**
   * Executes the given callable and performs additional exception handling/conversion.
   *
   * @param callable The callable to call
   * @param <T> The type of the callable.
   * @return The result of the callable
   * @throws NessieNotFoundException If the callable threw a gRPC exception, where the status
   *     matches a {@link NessieNotFoundException}
   * @throws NessieConflictException If the callable threw a gRPC exception, where the status
   *     matches a {@link NessieConflictException}
   * @throws NessieBadRequestException If the callable threw a gRPC exception, where the status
   *     matches a {@link NessieBadRequestException}
   * @throws StatusRuntimeException If the underlying exception couldn't be converted to the
   *     mentioned Nessie-specific exceptions, then a {@link StatusRuntimeException} with {@link
   *     Status#UNKNOWN} is thrown.
   */
  public static <T> T handle(Callable<T> callable)
      throws NessieNotFoundException, NessieConflictException {
    try {
      return callable.call();
    } catch (Exception e) {
      if (e instanceof StatusRuntimeException) {
        StatusRuntimeException sre = (StatusRuntimeException) e;
        if (isNotFound(sre)) {
          throw GrpcExceptionMapper.toNessieNotFoundException(sre);
        } else if (isAlreadyExists(sre)) {
          throw GrpcExceptionMapper.toNessieConflictException(sre);
        } else if (isInvalidArgument(sre)) {
          throw GrpcExceptionMapper.toNessieBadRequestException(sre);
        }
        throw sre;
      }
      throw Status.UNKNOWN.withCause(e).asRuntimeException();
    }
  }

  /**
   * Executes the given callable and performs additional exception handling/conversion.
   *
   * @param callable The callable to call
   * @param <T> The type of the callable.
   * @return The result of the callable
   * @throws NessieNotFoundException If the callable threw a gRPC exception, where the status
   *     matches a {@link NessieNotFoundException}
   * @throws NessieBadRequestException If the callable threw a gRPC exception, where the status
   *     matches a {@link NessieBadRequestException}
   * @throws StatusRuntimeException If the underlying exception couldn't be converted to the
   *     mentioned Nessie-specific exceptions, then a {@link StatusRuntimeException} with {@link
   *     Status#UNKNOWN} is thrown.
   */
  public static <T> T handleNessieNotFoundEx(Callable<T> callable) throws NessieNotFoundException {
    try {
      return callable.call();
    } catch (Exception e) {
      if (e instanceof StatusRuntimeException) {
        StatusRuntimeException sre = (StatusRuntimeException) e;
        if (isNotFound(sre)) {
          throw GrpcExceptionMapper.toNessieNotFoundException(sre);
        } else if (isInvalidArgument(sre)) {
          throw GrpcExceptionMapper.toNessieBadRequestException(sre);
        }
        throw sre;
      }
      throw Status.UNKNOWN.withCause(e).asRuntimeException();
    }
  }

  private static boolean isInvalidArgument(StatusRuntimeException sre) {
    return Status.INVALID_ARGUMENT.getCode().equals(sre.getStatus().getCode());
  }

  private static boolean isNotFound(StatusRuntimeException sre) {
    return Status.NOT_FOUND.getCode().equals(sre.getStatus().getCode());
  }

  private static boolean isAlreadyExists(StatusRuntimeException sre) {
    return Status.ALREADY_EXISTS.getCode().equals(sre.getStatus().getCode());
  }

  private static NessieNotFoundException toNessieNotFoundException(StatusRuntimeException e) {
    return new NessieNotFoundException(e.getMessage(), e.getCause());
  }

  private static NessieConflictException toNessieConflictException(StatusRuntimeException e) {
    return new NessieConflictException(e.getMessage(), e.getCause());
  }

  private static NessieBadRequestException toNessieBadRequestException(StatusRuntimeException e) {
    return new NessieBadRequestException(
        new NessieError(
            e.getMessage(),
            BAD_REQUEST.getStatusCode(),
            BAD_REQUEST.getReasonPhrase(),
            Throwables.getStackTraceAsString(e),
            (Exception) e.getCause()));
  }
}
