/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.client.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class HttpTestUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private HttpTestUtil() {}

  public static void writeEmptyResponse(HttpServletResponse resp) throws IOException {
    writeResponseBody(resp, "", "text/plain");
  }

  public static void writeResponseBody(HttpServletResponse resp, String response)
      throws IOException {
    writeResponseBody(resp, response, "application/json");
  }

  public static void writeResponseBody(HttpServletResponse resp, Object body, String contentType)
      throws IOException {
    writeResponseBody(resp, body, contentType, 200);
  }

  public static void writeResponseBody(
      HttpServletResponse resp, Object body, String contentType, int statusCode)
      throws IOException {
    writeResponseBody(resp, MAPPER.writer().writeValueAsBytes(body), contentType, statusCode);
  }

  public static void writeResponseBody(
      HttpServletResponse resp, String response, String contentType) throws IOException {
    writeResponseBody(resp, response.getBytes(StandardCharsets.UTF_8), contentType);
  }

  public static void writeResponseBody(HttpServletResponse resp, byte[] body, String contentType)
      throws IOException {
    writeResponseBody(resp, body, contentType, 200);
  }

  public static void writeResponseBody(
      HttpServletResponse resp, byte[] body, String contentType, int statusCode)
      throws IOException {
    resp.addHeader("Content-Type", contentType);
    resp.setContentLength(body.length);
    resp.setStatus(statusCode);
    try (OutputStream os = resp.getOutputStream()) {
      os.write(body);
    }
  }
}
