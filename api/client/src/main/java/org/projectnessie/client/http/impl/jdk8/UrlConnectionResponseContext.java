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
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
import org.projectnessie.client.http.impl.ResponseClosingInputStream;

final class UrlConnectionResponseContext implements ResponseContext {

  private final HttpURLConnection connection;
  private final URI uri;
  private final Status status;
  private InputStream inputStream;

  UrlConnectionResponseContext(HttpURLConnection connection, URI uri, Status status) {
    this.connection = connection;
    this.uri = uri;
    this.status = status;
  }

  @Override
  public Status getStatus() {
    return status;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    InputStream base = safeGetInputStream();
    if (base == null) {
      return null;
    }
    if (inputStream == null) {
      inputStream = new ResponseClosingInputStream(maybeDecompress(base), connection::disconnect);
    }
    return inputStream;
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
  public void close(Exception error) {
    if (error != null) {
      try {
        InputStream base = safeGetInputStream();
        if (base != null) {
          base.close();
        }
      } catch (Exception e) {
        error.addSuppressed(e);
      } finally {
        connection.disconnect();
      }
    }
  }

  private InputStream safeGetInputStream() throws IOException {
    try {
      return status.getCode() >= 400 ? connection.getErrorStream() : connection.getInputStream();
    } catch (IOException e) {
      connection.disconnect();
      throw e;
    }
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
