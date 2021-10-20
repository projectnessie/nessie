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
package org.projectnessie.error;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

class TestNessieError {

  @Test
  void allNulls() {
    NessieError e = new NessieError(0, null, null, null);
    assertNull(e.getMessage());
  }

  @Test
  void userConstructor() {
    NessieError e =
        new NessieError(
            "message",
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
            Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase(),
            "foo.bar.InternalServerError\n" + "\tat some.other.Class",
            new Exception("processingException"));
    assertEquals("message", e.getMessage());
    assertThat(e.getErrorCode()).isEqualTo(ErrorCode.UNKNOWN);
    assertThat(e.getFullMessage())
        .startsWith(
            Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase()
                + " (HTTP/"
                + Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
                + "): message\n"
                + "foo.bar.InternalServerError\n"
                + "\tat some.other.Class\n"
                + "java.lang.Exception: processingException\n"
                + "\tat org.projectnessie.error.TestNessieError.userConstructor(TestNessieError.java:");

    e =
        new NessieError(
            "message",
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
            Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase(),
            "foo.bar.InternalServerError\n" + "\tat some.other.Class");
    assertThat(e.getReason()).isEqualTo(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase());
    assertThat(e.getErrorCode()).isEqualTo(ErrorCode.UNKNOWN);
    assertThat(e.getFullMessage())
        .startsWith(
            Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase()
                + " (HTTP/"
                + Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
                + "): message\n");
  }

  @Test
  void jsonCreator() {
    NessieError e =
        new NessieError(
            "message",
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
            ErrorCode.CONTENTS_NOT_FOUND,
            Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase(),
            "foo.bar.InternalServerError\n" + "\tat some.other.Class");
    assertAll(
        () ->
            assertEquals(
                Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase()
                    + " (HTTP/"
                    + Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
                    + "): message\n"
                    + "foo.bar.InternalServerError\n"
                    + "\tat some.other.Class",
                e.getFullMessage()),
        () -> assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getStatus()),
        () -> assertEquals("message", e.getMessage()),
        () -> assertEquals(ErrorCode.CONTENTS_NOT_FOUND, e.getErrorCode()),
        () ->
            assertEquals(
                "foo.bar.InternalServerError\n" + "\tat some.other.Class",
                e.getServerStackTrace()));
  }

  @Test
  void testEquals() {
    NessieError e1 = new NessieError("m", 1, "r", null);
    NessieError e2 = new NessieError("m", 1, ErrorCode.UNKNOWN, "r", null);
    NessieError e3 = new NessieError("m", 1, ErrorCode.REFERENCE_NOT_FOUND, "r", null);

    assertThat(e1).isEqualTo(e2);
    assertThat(e1.hashCode()).isEqualTo(e2.hashCode());

    assertThat(e1).isNotEqualTo(e3);
  }
}
