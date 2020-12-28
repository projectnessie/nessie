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
package com.dremio.nessie.server.error;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.ResponseProcessingException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.dremio.nessie.client.rest.NessieBadRequestException;
import com.dremio.nessie.client.rest.ObjectMapperContextResolver;
import com.dremio.nessie.client.rest.ResponseCheckFilter;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.ContentsKey.NessieObjectKeyConverterProvider;

import io.quarkus.test.junit.QuarkusTest;

/**
 * Test reported exceptions both for cases when {@code javax.validation} fails (when the Nessie infra
 * code isn't even run) and exceptions reported <em>by</em> Nessie.
 */
@QuarkusTest
class TestNessieError {

  static String baseURI = "http://localhost:19121/api/v1/nessieErrorTest";

  private static WebTarget target;

  @BeforeAll
  static void setup() {
    target = ClientBuilder.newBuilder()
                          .register(ObjectMapperContextResolver.class)
                          .register(ResponseCheckFilter.class)
                          .register(NessieObjectKeyConverterProvider.class)
                          .build()
                          .target(baseURI);
  }

  @Test
  void nullParameterQueryGet() {
    assertEquals("Bad Request (HTTP/400): parameter nullParameterQueryGet.hash must not be null",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       target.path("nullParameterQueryGet")
                             .request()
                             .get()).getMessage());
  }

  @Test
  void nullParameterQueryPost() {
    assertEquals("Bad Request (HTTP/400): parameter nullParameterQueryPost.hash must not be null",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       target.path("nullParameterQueryPost")
                             .request()
                             .post(Entity.entity("", MediaType.TEXT_PLAIN_TYPE))).getMessage());
  }

  @Test
  void emptyParameterQueryGet() {
    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): parameter emptyParameterQueryGet.hash must not be empty",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       target.path("emptyParameterQueryGet")
                             .request()
                             .get()).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): parameter emptyParameterQueryGet.hash must not be empty",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       target.path("emptyParameterQueryGet")
                             .queryParam("hash", "")
                             .request()
                             .get()).getMessage())
    );
  }

  @Test
  void blankParameterQueryGet() {
    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): parameter blankParameterQueryGet.hash must not be blank",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       target.path("blankParameterQueryGet")
                             .request()
                             .get()).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): parameter blankParameterQueryGet.hash must not be blank",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       target.path("blankParameterQueryGet")
                             .queryParam("hash", "")
                             .request()
                             .get()).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): parameter blankParameterQueryGet.hash must not be blank (value='   ')",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       target.path("blankParameterQueryGet")
                             .queryParam("hash", "   ")
                             .request()
                             .get()).getMessage())
    );
  }

  @Test
  void entityValueViolation() {
    assertAll(
        () -> assertThat(assertThrows(NessieBadRequestException.class,
          () ->
              target.path("basicEntity")
                    .request()
                    .put(Entity.entity("not really valid json", MediaType.APPLICATION_JSON_TYPE))
         ).getMessage(),
         startsWith("Bad Request (HTTP/400): Unrecognized token 'not': was expecting (JSON String, Number, "
                    + "Array, Object or token 'null', 'true' or 'false')\n")),
        () -> assertThat(assertThrows(NessieBadRequestException.class,
          () ->
              target.path("basicEntity")
                    .request()
                    .put(Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE))
         ).getMessage(),
         startsWith("Bad Request (HTTP/400): Missing required creator property 'value' (index 0)\n")),
        () -> assertThat(assertThrows(NessieBadRequestException.class,
          () ->
              target.path("basicEntity")
                    .request()
                    .put(Entity.entity("{\"value\":null}", MediaType.APPLICATION_JSON_TYPE))
         ).getMessage(),
         equalTo("Bad Request (HTTP/400): parameter basicEntity.entity.value must not be null")),
        () -> assertThat(assertThrows(NessieBadRequestException.class,
          () ->
              target.path("basicEntity")
                    .request()
                    .put(Entity.entity("{\"value\":1.234}", MediaType.APPLICATION_JSON_TYPE))
         ).getMessage(),
         equalTo("Bad Request (HTTP/400): parameter basicEntity.entity.value must be greater than or equal to 3 (value='1')"))
    );
  }

  @Test
  void brokenEntitySerialization() {
    // send something that cannot be deserialized
    assertThat(assertThrows(NessieBadRequestException.class,
        () ->
            target.path("basicEntity")
                  .request()
                  .put(Entity.entity(new OtherEntity("bar"), MediaType.APPLICATION_JSON_TYPE))
       ).getMessage(),
        startsWith("Bad Request (HTTP/400): Missing required creator property 'value' (index 0)\n"));
  }

  @Test
  void nessieNotFoundException() {
    NessieNotFoundException ex = assertThrows(NessieNotFoundException.class,
        () -> unwrap(() ->
            target.path("nessieNotFound")
                  .request()
                  .get()));
    assertAll(
        () -> assertEquals("not-there-message",
                           ex.getMessage()),
        () -> assertThat(ex.getServerStackTrace(),
                         startsWith("com.dremio.nessie.error.NessieNotFoundException: not-there-message\n")),
        () -> assertEquals(Response.Status.NOT_FOUND, ex.getStatus())
    );
  }

  void unwrap(Executable exec) throws Throwable {
    try {
      exec.execute();
    } catch (Throwable targetException) {
      if (targetException instanceof ResponseProcessingException) {
        if (targetException.getCause() instanceof NessieNotFoundException
            || targetException.getCause() instanceof NessieConflictException) {
          throw targetException.getCause();
        }
      }

      throw targetException;
    }
  }
}
