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

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Simple Http client to make REST calls.
 *
 * <p>
 *   Assumptions:
 *   - always send/receive JSON
 *   - set headers accordingly by default
 *   - very simple interactions w/ API
 *   - no cookies
 *   - no caching of connections. Could be slow
 * </p>
 */
public class HttpClient {
  private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
                                                        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
  private final String baseUri;
  private final String accept;
  private final List<RequestFilter> requestFilters = new ArrayList<>();
  private final List<ResponseFilter> responseFilters = new ArrayList<>();

  public enum Method {
    GET,
    POST,
    PUT,
    DELETE;
  }

  /**
   * Construct an HTTP client with a universal Accept header.
   *
   * @param baseUri uri base eg https://example.com
   * @param accept Accept header eg "application/json"
   */
  public HttpClient(String baseUri, String accept) {
    this.baseUri = HttpUtils.checkNonNullTrim(baseUri);
    this.accept = HttpUtils.checkNonNullTrim(accept);
    HttpUtils.checkArgument(baseUri.startsWith("http://") || baseUri.startsWith("https://"),
                            "Cannot start http client. %s must be a valid http or https address", baseUri);
  }

  public HttpClient(String baseUri) {
    this(baseUri, "application/json");
  }

  public void register(RequestFilter filter) {
    filter.init(mapper);
    requestFilters.add(filter);
  }

  public void register(ResponseFilter filter) {
    filter.init(mapper);
    responseFilters.add(filter);
  }

  public HttpRequest create() {
    return new HttpRequest(baseUri, accept, mapper, requestFilters, responseFilters);
  }
}
