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
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
import org.projectnessie.client.http.impl.ResponseClosingInputStream;

final class ApacheResponseContext implements ResponseContext {

  private final ClassicHttpResponse response;
  private final URI uri;
  private InputStream inputStream;

  ApacheResponseContext(ClassicHttpResponse response, URI uri) {
    this.response = response;
    this.uri = uri;
  }

  @Override
  public Status getStatus() {
    return Status.fromCode(response.getCode());
  }

  @Override
  public InputStream getInputStream() throws IOException {
    InputStream base = safeGetInputStream();
    if (base == null) {
      return null;
    }
    if (inputStream == null) {
      inputStream = new ResponseClosingInputStream(base, response);
    }
    return inputStream;
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

  @Override
  public void close(Exception error) {
    if (error != null) {
      try {
        EntityUtils.consume(response.getEntity());
      } catch (IOException e) {
        error.addSuppressed(e);
      } finally {
        try {
          response.close();
        } catch (IOException e) {
          error.addSuppressed(e);
        }
      }
    }
  }

  private InputStream safeGetInputStream() throws IOException {
    try {
      HttpEntity entity = response.getEntity();
      if (entity == null) {
        return null;
      }
      return entity.getContent();
    } catch (IOException e) {
      try {
        response.close();
      } catch (IOException ex) {
        e.addSuppressed(ex);
      }
      throw e;
    }
  }
}
