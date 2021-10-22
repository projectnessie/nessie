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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

class TestNessieError {

  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  void fullMessage() {
    NessieError e =
        ImmutableNessieError.builder()
            .message("message")
            .errorCode(ErrorCode.UNKNOWN)
            .status(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
            .reason(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())
            .serverStackTrace("foo.bar.InternalServerError\n" + "\tat some.other.Class")
            .build();
    assertThat(e.getFullMessage())
        .isEqualTo(
            Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase()
                + " (HTTP/"
                + Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
                + "): message\n"
                + "foo.bar.InternalServerError\n"
                + "\tat some.other.Class");

    e =
        ImmutableNessieError.builder()
            .from(e)
            .clientProcessingException(new Exception("processingException"))
            .build();
    assertThat(e.getFullMessage())
        .startsWith(
            Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase()
                + " (HTTP/"
                + Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
                + "): message\n"
                + "foo.bar.InternalServerError\n"
                + "\tat some.other.Class\n"
                + "java.lang.Exception: processingException\n"
                + "\tat org.projectnessie.error.TestNessieError.fullMessage(TestNessieError.java:");
  }

  @Test
  void jsonRoundTrip() throws JsonProcessingException {
    NessieError e0 =
        ImmutableNessieError.builder()
            .message("message")
            .errorCode(ErrorCode.UNKNOWN)
            .status(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
            .reason(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())
            .serverStackTrace("foo.bar.InternalServerError\n" + "\tat some.other.Class")
            .clientProcessingException(new Exception("processingException"))
            .build();

    String json = mapper.writeValueAsString(e0);
    NessieError e1 = mapper.readValue(json, NessieError.class);

    assertThat(e1.getClientProcessingException()).isNull(); // not propagated through JSON

    // Copy e0 without the client error
    NessieError e2 =
        ImmutableNessieError.builder().from(e0).clientProcessingException(null).build();
    assertThat(e1).isEqualTo(e2);
  }
}
