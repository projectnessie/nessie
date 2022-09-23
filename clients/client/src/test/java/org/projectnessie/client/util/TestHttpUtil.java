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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;

public class TestHttpUtil {

  private TestHttpUtil() {}

  public static void emptyResponse(HttpServletResponse resp) throws IOException {
    writeResponseBody(resp, "", "text/plain");
  }

  public static void writeResponseBody(HttpServletResponse resp, String response)
      throws IOException {
    writeResponseBody(resp, response, "application/json");
  }

  public static void writeResponseBody(
      HttpServletResponse resp, String response, String contentType) throws IOException {
    resp.addHeader("Content-Type", contentType);
    byte[] body = response.getBytes(StandardCharsets.UTF_8);
    resp.setContentLength(body.length);
    resp.setStatus(200);
    try (OutputStream os = resp.getOutputStream()) {
      os.write(body);
    }
  }
}
