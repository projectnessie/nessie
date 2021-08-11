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

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.client.grpc.GrpcExceptionMapper.toProto;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Collections;
import org.jboss.resteasy.api.validation.ResteasyViolationException;
import org.jboss.resteasy.spi.ResteasyConfiguration;
import org.jboss.resteasy.spi.validation.ConstraintTypeUtil;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;

public class TestGrpcExceptionMapper {

  @Test
  public void exceptionToProtoConversion() {
    assertThat(toProto(new NessieNotFoundException("not found")))
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(e -> ((StatusRuntimeException) e).getStatus().getCode())
        .isEqualTo(Status.NOT_FOUND.getCode());

    assertThat(toProto(new NessieConflictException("conflict")))
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(e -> ((StatusRuntimeException) e).getStatus().getCode())
        .isEqualTo(Status.ALREADY_EXISTS.getCode());

    Arrays.asList(
            new JsonParseException(null, "x"),
            new JsonMappingException(null, "x"),
            new IllegalArgumentException("x"))
        .forEach(
            ex -> {
              assertThat(toProto(ex))
                  .isInstanceOf(StatusRuntimeException.class)
                  .extracting(e -> ((StatusRuntimeException) e).getStatus().getCode())
                  .isEqualTo(Status.INVALID_ARGUMENT.getCode());
            });

    assertThat(toProto(new AccessControlException("not allowed")))
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(e -> ((StatusRuntimeException) e).getStatus().getCode())
        .isEqualTo(Status.PERMISSION_DENIED.getCode());

    assertThat(
            toProto(
                new ResteasyViolationException(Collections.emptySet()) {
                  @Override
                  public ConstraintTypeUtil getConstraintTypeUtil() {
                    return null;
                  }

                  @Override
                  protected ResteasyConfiguration getResteasyConfiguration() {
                    return null;
                  }
                }))
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(e -> ((StatusRuntimeException) e).getStatus().getCode())
        .isEqualTo(Status.INVALID_ARGUMENT.getCode());

    // everything else maps to INTERNAL
    assertThat(toProto(new NullPointerException()))
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(e -> ((StatusRuntimeException) e).getStatus().getCode())
        .isEqualTo(Status.INTERNAL.getCode());
  }
}
