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
package org.projectnessie.client.http.impl.jdk8;

import static org.projectnessie.client.http.impl.HttpUtils.DEFLATE;
import static org.projectnessie.client.http.impl.HttpUtils.GZIP;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_CONTENT_ENCODING;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_CONTENT_TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;

final class UrlConnectionResponseContext implements ResponseContext {

  private final HttpURLConnection connection;
  private final URI uri;
  private final Method method;

  UrlConnectionResponseContext(HttpURLConnection connection, URI uri, Method method) {
    this.connection = connection;
    this.uri = uri;
    this.method = method;
  }

  @Override
  public Status getResponseCode() throws IOException {
    return Status.fromCode(connection.getResponseCode());
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return maybeDecompress(connection.getInputStream());
  }

  @Override
  public InputStream getErrorStream() throws IOException {
    return maybeDecompress(connection.getErrorStream());
  }

  @Override
  public String getContentType() {
    return connection.getHeaderField(HEADER_CONTENT_TYPE);
  }

  @Override
  public URI getRequestedUri() {
    return uri;
  }

  @Override
  public Method getRequestedMethod() {
    return method;
  }

  private InputStream maybeDecompress(InputStream inputStream) throws IOException {
    String contentEncoding = connection.getHeaderField(HEADER_CONTENT_ENCODING);
    if (GZIP.equals(contentEncoding)) {
      return new GZIPInputStream(inputStream);
    } else if (DEFLATE.equals(contentEncoding)) {
      return new InflaterInputStream(inputStream);
    } else {
      return inputStream;
    }
  }
}
