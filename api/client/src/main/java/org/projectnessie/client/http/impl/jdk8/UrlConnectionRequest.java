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

import static org.projectnessie.client.http.impl.HttpUtils.applyHeaders;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import javax.net.ssl.HttpsURLConnection;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
import org.projectnessie.client.http.impl.BaseHttpRequest;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;

/** Class to hold an ongoing HTTP request and its parameters/filters. */
final class UrlConnectionRequest extends BaseHttpRequest {

  UrlConnectionRequest(HttpRuntimeConfig config, URI baseUri) {
    super(config, baseUri);
  }

  @Override
  protected ResponseContext sendAndReceive(
      URI uri, Method method, Object body, RequestContext requestContext) throws IOException {
    HttpURLConnection con = (HttpURLConnection) uri.toURL().openConnection();
    con.setReadTimeout(config.getReadTimeoutMillis());
    con.setConnectTimeout(config.getConnectionTimeoutMillis());
    if (con instanceof HttpsURLConnection) {
      ((HttpsURLConnection) con).setSSLSocketFactory(config.getSslContext().getSocketFactory());
    }

    applyHeaders(headers, con);
    con.setRequestMethod(method.name());

    if (requestContext.doesOutput()) {
      con.setDoOutput(true);
      writeToOutputStream(requestContext, con.getOutputStream());
    }
    con.connect();
    Status status = Status.fromCode(con.getResponseCode());
    return new UrlConnectionResponseContext(con, uri, status);
  }
}
