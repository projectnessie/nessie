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
package com.dremio.nessie.client.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.nessie.client.http.HttpClient.Method;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Class to hold an ongoing HTTP request and its parameters/filters.
 */
public class HttpRequest {

  private final UriBuilder uriBuilder;
  private final ObjectMapper mapper;
  private final Map<String, String> headers = new HashMap<>();
  private final List<RequestFilter> requestFilters;
  private final List<ResponseFilter> responseFilters;

  HttpRequest(String base, String accept, ObjectMapper mapper, List<RequestFilter> requestFilters, List<ResponseFilter> responseFilters) {
    this.uriBuilder = new UriBuilder(base);
    this.mapper = mapper;
    this.headers.put("Accept", accept);
    this.requestFilters = requestFilters;
    this.responseFilters = responseFilters;
  }

  public HttpRequest path(String path) {
    this.uriBuilder.path(path);
    return this;
  }

  public HttpRequest queryParam(String name, String value) {
    this.uriBuilder.queryParam(name, value);
    return this;
  }

  public HttpRequest header(String name, String value) {
    this.headers.put(name, value);
    return this;
  }

  private HttpResponse getCon(Method method, Object body) throws HttpClientException {
    try {
      String uri = uriBuilder.build();
      URL url = new URL(uri);
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      RequestContext context = new RequestContext(headers, uri, method, body);
      requestFilters.forEach(a -> a.filter(context));
      headers.forEach(con::setRequestProperty);
      con.setRequestMethod(method.name());
      if (body != null && (method.equals(Method.PUT) || method.equals(Method.POST))) {
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        mapper.writerFor(body.getClass()).writeValue(con.getOutputStream(), body);
      }
      ResponseContext responseContext = new ResponseContextImpl(con);
      responseFilters.forEach(a -> a.filter(responseContext));
      return new HttpResponse(context, responseContext, mapper);
    } catch (IOException e) {
      throw new HttpClientException(e);
    }
  }

  public HttpResponse get() throws HttpClientException {
    return getCon(Method.GET, null);
  }

  public HttpResponse delete() throws HttpClientException {
    return getCon(Method.DELETE, null);
  }

  public HttpResponse post(Object obj) throws HttpClientException {
    return getCon(Method.POST, obj);
  }

  public HttpResponse put(Object obj) throws HttpClientException {
    return getCon(Method.PUT, obj);
  }

  public HttpRequest resolveTemplate(String name, String value) {
    uriBuilder.resolveTemplate(name, value);
    return this;
  }
}
