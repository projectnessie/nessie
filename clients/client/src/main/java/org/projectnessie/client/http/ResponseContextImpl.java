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
package org.projectnessie.client.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;

class ResponseContextImpl implements ResponseContext {

  private final HttpURLConnection connection;

  ResponseContextImpl(HttpURLConnection connection) {
    this.connection = connection;
  }

  public Status getResponseCode() throws IOException {
    return Status.fromCode(connection.getResponseCode());
  }

  public InputStream getInputStream() throws IOException {
    return connection.getInputStream();
  }

  public InputStream getErrorStream() {
    return connection.getErrorStream();
  }
}
