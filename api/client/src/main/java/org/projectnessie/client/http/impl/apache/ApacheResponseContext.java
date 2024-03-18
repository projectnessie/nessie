/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.client.http.impl.apache;

import static org.projectnessie.client.http.impl.HttpUtils.HEADER_CONTENT_TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.Header;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;

final class ApacheResponseContext implements ResponseContext {

  private final CloseableHttpResponse response;
  private URI uri;

  ApacheResponseContext(CloseableHttpResponse response, URI uri) {
    this.response = response;
    this.uri = uri;
  }

  @Override
  public Status getResponseCode() {
    return Status.fromCode(response.getCode());
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return reader();
  }

  @Override
  public InputStream getErrorStream() throws IOException {
    return reader();
  }

  @Override
  public boolean isJsonCompatibleResponse() {
    String contentType = getContentType();
    if (contentType == null) {
      return false;
    }
    int i = contentType.indexOf(';');
    if (i > 0) {
      contentType = contentType.substring(0, i);
    }
    return contentType.endsWith("/json") || contentType.endsWith("+json");
  }

  @Override
  public String getContentType() {
    return headerValue(HEADER_CONTENT_TYPE);
  }

  @Override
  public URI getRequestedUri() {
    return uri;
  }

  private String headerValue(String name) {
    Header header = response.getFirstHeader(name);
    return header != null ? header.getValue() : null;
  }

  private InputStream reader() throws IOException {
    return new RequestClosingInputStream(response);
  }

  private static final class RequestClosingInputStream extends InputStream {
    private final InputStream in;
    private final CloseableHttpResponse response;

    RequestClosingInputStream(CloseableHttpResponse response) throws IOException {
      this.response = response;
      try {
        this.in = response.getEntity().getContent();
      } catch (IOException e) {
        response.close();
        throw e;
      }
    }

    @Override
    public int read() throws IOException {
      return in.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
      return in.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
      return in.skip(n);
    }

    @Override
    public int available() throws IOException {
      return in.available();
    }

    @Override
    public void close() throws IOException {
      try {
        in.close();
      } finally {
        response.close();
      }
    }
  }
}
