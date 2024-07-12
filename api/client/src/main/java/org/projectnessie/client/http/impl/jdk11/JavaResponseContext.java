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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements Nessie HTTP response processing using Java's new {@link HttpClient}, which is rather
 * straight forward, because we already have a "good old" {@link InputStream} at hand.
 */
@SuppressWarnings("Since15") // IntelliJ warns about new APIs. 15 is misleading, it means 11
final class JavaResponseContext implements ResponseContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(JavaResponseContext.class);

  private final HttpResponse<InputStream> response;
  private InputStream inputStream;

  JavaResponseContext(HttpResponse<InputStream> response) {
    this.response = response;
  }

  @Override
  public Status getStatus() {
    return Status.fromCode(response.statusCode());
  }

  @Override
  public InputStream getInputStream() throws IOException {
    if (inputStream == null) {
      inputStream = maybeDecompress();
    }
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

  public void close(Exception error) {
    if (error != null) {
      try {
        LOGGER.debug(
            "Closing unprocessed input stream for {} request to {} delegating to {} ...",
            response.request().method(),
            response.uri(),
            response.body());
        response.body().close();
      } catch (IOException e) {
        error.addSuppressed(e);
      }
    }
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
