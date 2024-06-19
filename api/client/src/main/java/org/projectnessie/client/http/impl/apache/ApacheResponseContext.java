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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;

final class ApacheResponseContext implements ResponseContext {

  private final ClassicHttpResponse response;
  private final URI uri;

  ApacheResponseContext(ClassicHttpResponse response, URI uri) {
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
    Header header = response.getFirstHeader(HEADER_CONTENT_TYPE);
    return header != null ? header.getValue() : null;
  }

  @Override
  public URI getRequestedUri() {
    return uri;
  }

  private InputStream reader() throws IOException {
    return new RequestClosingInputStream(response);
  }

  private static final class RequestClosingInputStream extends FilterInputStream {
    private final ClassicHttpResponse response;

    RequestClosingInputStream(ClassicHttpResponse response) throws IOException {
      super(closeOnFail(response));
      this.response = response;
    }

    private static InputStream closeOnFail(ClassicHttpResponse response) throws IOException {
      try {
        return response.getEntity().getContent();
      } catch (IOException e) {
        response.close();
        throw e;
      }
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        response.close();
      }
    }
  }
}
