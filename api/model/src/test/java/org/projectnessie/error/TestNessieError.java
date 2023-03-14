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
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
class TestNessieError {

  private static final ObjectMapper mapper = new ObjectMapper();
  public static final int HTTP_500_CODE = 500;
  public static final String HTTP_500_MESSAGE = "Internal Server Error";

  @Test
  void fullMessage() {
    NessieError e =
        ImmutableNessieError.builder()
            .message("message")
            .errorCode(ErrorCode.UNKNOWN)
            .status(HTTP_500_CODE)
            .reason(HTTP_500_MESSAGE)
            .serverStackTrace("foo.bar.InternalServerError\n" + "\tat some.other.Class")
            .build();
    assertThat(e.getFullMessage())
        .matches(
            HTTP_500_MESSAGE
                + " [(]HTTP/"
                + HTTP_500_CODE
                + "[)]: message\\R"
                + "foo.bar.InternalServerError\\R"
                + "\tat some.other.Class");

    e =
        ImmutableNessieError.builder()
            .from(e)
            .clientProcessingException(new Exception("processingException"))
            .build();
    assertThat(e.getFullMessage())
        .matches(
            // Using a regex here, because the stack trace looks different with
            // junit-platform-maven-plugin
            Pattern.compile(
                HTTP_500_MESSAGE
                    + " [(]HTTP/"
                    + HTTP_500_CODE
                    + "[)]: message\\R"
                    + "foo.bar.InternalServerError\\R"
                    + "\tat some.other.Class\\R"
                    + "java.lang.Exception: processingException\\R"
                    + "\tat (all//)?org.projectnessie.error.TestNessieError.fullMessage[(]TestNessieError.java:"
                    + ".*",
                Pattern.DOTALL));
  }

  @Test
  void jsonRoundTrip() throws JsonProcessingException {
    NessieError e0 =
        ImmutableNessieError.builder()
            .message("message")
            .errorCode(ErrorCode.UNKNOWN)
            .status(HTTP_500_CODE)
            .reason(HTTP_500_MESSAGE)
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
