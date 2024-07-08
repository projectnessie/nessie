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
package org.projectnessie.client.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.projectnessie.client.http.Status;

class TestOAuth2Exception {

  private final ImmutableErrorResponse errorResponse =
      ImmutableErrorResponse.builder()
          .errorCode("invalid_request")
          .errorDescription("Try Again")
          .build();
  private final ImmutableErrorResponse errorResponseNoDescription =
      ImmutableErrorResponse.builder().errorCode("invalid_request").build();

  @Test
  void testGetStatus() {
    OAuth2Exception exception = new OAuth2Exception("message", Status.BAD_REQUEST, errorResponse);
    assertThat(exception.getStatus()).isEqualTo(Status.BAD_REQUEST);
  }

  @Test
  void testGetErrorCode() {
    OAuth2Exception exception = new OAuth2Exception("message", Status.BAD_REQUEST, errorResponse);
    assertThat(exception.getErrorCode()).isEqualTo("invalid_request");
  }

  @Test
  void testGetErrorDescription() {
    OAuth2Exception exception = new OAuth2Exception("message", Status.BAD_REQUEST, errorResponse);
    assertThat(exception.getErrorDescription()).isEqualTo("Try Again");
    exception = new OAuth2Exception("message", Status.BAD_REQUEST, errorResponseNoDescription);
    assertThat(exception.getErrorDescription()).isNull();
  }
}
