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
package org.projectnessie.client.http.impl.jdk11;

import static org.projectnessie.client.http.impl.HttpUtils.DEFLATE;
import static org.projectnessie.client.http.impl.HttpUtils.GZIP;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_CONTENT_ENCODING;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_CONTENT_TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;

/**
 * Implements Nessie HTTP response processing using Java's new {@link HttpClient}, which is rather
 * straight forward, because we already have a "good old" {@link InputStream} at hand.
 */
@SuppressWarnings("Since15") // IntelliJ warns about new APIs. 15 is misleading, it means 11
final class JavaResponseContext implements ResponseContext {

  private final HttpResponse<InputStream> response;
  private final InputStream inputStream;
  private final org.projectnessie.client.http.HttpClient.Method method;

  JavaResponseContext(
      HttpResponse<InputStream> response, org.projectnessie.client.http.HttpClient.Method method) {
    this.response = response;
    this.method = method;

    try {
      this.inputStream = maybeDecompress();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Status getResponseCode() {
    return Status.fromCode(response.statusCode());
  }

  @Override
  public InputStream getInputStream() {
    return inputStream;
  }

  @Override
  public InputStream getErrorStream() {
    return inputStream;
  }

  @Override
  public String getContentType() {
    return response.headers().firstValue(HEADER_CONTENT_TYPE).orElse(null);
  }

  @Override
  public URI getRequestedUri() {
    return response.uri();
  }

  @Override
  public org.projectnessie.client.http.HttpClient.Method getRequestedMethod() {
    return method;
  }

  private InputStream maybeDecompress() throws IOException {
    InputStream base = response.body();
    String contentEncoding = response.headers().firstValue(HEADER_CONTENT_ENCODING).orElse("");
    if (GZIP.equals(contentEncoding)) {
      return new GZIPInputStream(base);
    } else if (DEFLATE.equals(contentEncoding)) {
      return new InflaterInputStream(base);
    } else {
      return base;
    }
  }
}
