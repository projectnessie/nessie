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
package com.dremio.nessie.error;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import javax.ws.rs.core.Response;

import org.junit.jupiter.api.Test;

class TestNessieError {
  @Test
  void allNulls() {
    NessieError e = new NessieError(null, null, null);
    assertNull(e.getMessage());
  }

  @Test
  void userContructor() {
    NessieError e = new NessieError(
        "message",
        Response.Status.INTERNAL_SERVER_ERROR,
        "foo.bar.InternalServerError\n"
        + "\tat some.other.Class",
        new Exception("processingException"));
    assertEquals(
        "message",
        e.getMessage());
    assertThat(
        e.getFullMessage(),
        startsWith(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase()
                   + " (HTTP/" + Response.Status.INTERNAL_SERVER_ERROR.getStatusCode() + "): message\n"
                   + "foo.bar.InternalServerError\n"
                   + "\tat some.other.Class\n"
                   + "java.lang.Exception: processingException\n"
                   + "\tat com.dremio.nessie.error.TestNessieError.userContructor(TestNessieError.java:"));
  }

  @Test
  void jsonCreator() {
    NessieError e = new NessieError(
        "message",
        Response.Status.INTERNAL_SERVER_ERROR,
        "foo.bar.InternalServerError\n"
        + "\tat some.other.Class");
    assertAll(
        () -> assertEquals(
            Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase()
            + " (HTTP/" + Response.Status.INTERNAL_SERVER_ERROR.getStatusCode() + "): message\n"
            + "foo.bar.InternalServerError\n"
            + "\tat some.other.Class",
            e.getFullMessage()),
        () -> assertEquals(Response.Status.INTERNAL_SERVER_ERROR, e.getStatus()),
        () -> assertEquals("message",
                           e.getMessage()),
        () -> assertEquals(
            "foo.bar.InternalServerError\n"
            + "\tat some.other.Class",
            e.getServerStackTrace())
    );
  }

}
