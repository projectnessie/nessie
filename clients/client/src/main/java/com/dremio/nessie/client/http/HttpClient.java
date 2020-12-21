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

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
public class HttpClient implements AutoCloseable {
  private final ObjectMapper mapper;
  private final String base;
  private final String accept;
  private final List<RequestFilter> requestFilters = new ArrayList<>();
  private final List<ResponseFilter> responseFilters = new ArrayList<>();
  private final Set<ResponseContext> openResponses = new HashSet<>();
  private final ReferenceQueue<ResponseContext> referenceQueue = new ReferenceQueue<>();
  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  public enum Method {
    GET,
    POST,
    PUT,
    DELETE;
  }

  /**
   * Construct an HTTP client with a universal Accept header.
   *
   * @param base uri base eg https://example.com
   * @param accept Accept header eg "application/json"
   */
  public HttpClient(String base, String accept) {
    this.mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
                                    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    this.base = base;
    this.accept = accept;
    executor.execute(() -> {
      while (true) {
        try {
          HttpClientReference c = (HttpClientReference) referenceQueue.remove();
          c.close();
        } catch (InterruptedException e) {
          break;
        }
      }
    });

  }

  public HttpClient(String base) {
    this(base, "application/json");
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
    return new HttpRequest(base, accept, mapper, requestFilters, responseFilters,
        c -> new HttpClientReference(c, referenceQueue, openResponses));
  }

  @Override
  public void close() {
    openResponses.forEach(ResponseContext::close);
    openResponses.clear();
    executor.shutdown();
  }

  private static class HttpClientReference extends PhantomReference<ResponseContext> {
    private final ResponseContext referent;
    private final Set<ResponseContext> openResponses;

    public HttpClientReference(ResponseContext referent, ReferenceQueue<? super ResponseContext> q, Set<ResponseContext> openResponses) {
      super(referent, q);
      this.referent = referent;
      this.openResponses = openResponses;
      openResponses.add(referent);
    }

    public void close() {
      if (openResponses.remove(referent)) {
        referent.close();
      }
    }
  }
}
