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

import static org.projectnessie.client.http.impl.HttpUtils.HEADER_ACCEPT;

import java.io.IOException;
import java.net.URI;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.impl.BaseHttpRequest;
import org.projectnessie.client.http.impl.HttpHeaders.HttpHeader;

final class ApacheRequest extends BaseHttpRequest {

  private final ApacheHttpClient client;

  ApacheRequest(ApacheHttpClient apacheHttpClient, URI baseUri) {
    super(apacheHttpClient.config, baseUri);
    this.client = apacheHttpClient;
  }

  @Override
  protected ResponseContext sendAndReceive(
      URI uri, Method method, Object body, RequestContext requestContext) throws IOException {

    HttpUriRequestBase request = new HttpUriRequestBase(method.name(), uri);

    for (HttpHeader header : headers.allHeaders()) {
      for (String value : header.getValues()) {
        request.addHeader(header.getName(), value);
      }
    }

    request.addHeader(HEADER_ACCEPT, accept);
    if (requestContext.doesOutput()) {
      HttpEntity entity =
          HttpEntities.create(
              os -> writeToOutputStream(requestContext, os), ContentType.parse(contentsType));
      request.setEntity(entity);
    }

    ClassicHttpResponse response = client.client.executeOpen(null, request, null);
    return new ApacheResponseContext(response, uri);
  }
}
