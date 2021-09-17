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
package org.projectnessie.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.propagation.Format.Builtin;
import io.opentracing.propagation.TextMap;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.projectnessie.api.http.HttpConfigApi;
import org.projectnessie.api.http.HttpContentsApi;
import org.projectnessie.api.http.HttpTreeApi;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;

public class NessieHttpClient extends NessieApiClient {

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(SerializationFeature.INDENT_OUTPUT)
          .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  private final HttpClient client;

  /**
   * Create new HTTP Nessie client. All REST api endpoints are mapped here. This client should
   * support any jaxrs implementation
   *
   * @param uri URL for the nessie client (eg http://localhost:19120/api/v1)
   * @param authentication authenticator to use
   * @param readTimeout time in milliseconds to wait for a response from the server
   * @param connectionTimeoutMillis time in milliseconds to wait to connect to the server
   */
  NessieHttpClient(
      URI uri,
      HttpAuthentication authentication,
      boolean enableTracing,
      int readTimeout,
      int connectionTimeoutMillis) {
    this(
        HttpClient.builder()
            .setBaseUri(uri)
            .setObjectMapper(MAPPER)
            .setReadTimeoutMillis(readTimeout)
            .setConnectionTimeoutMillis(connectionTimeoutMillis)
            .build());
    if (enableTracing) {
      addTracing(client);
    }
    if (authentication != null) {
      authentication.applyToHttpClient(client);
    }
    client.register(new NessieHttpResponseFilter(MAPPER));
  }

  private NessieHttpClient(HttpClient client) {
    super(
        wrap(HttpConfigApi.class, new HttpConfigClient(client)),
        wrap(HttpTreeApi.class, new HttpTreeClient(client)),
        wrap(HttpContentsApi.class, new HttpContentsClient(client)));
    this.client = client;
  }

  private static void addTracing(HttpClient httpClient) {
    // It's safe to reference `GlobalTracer` here even without the required dependencies available
    // at runtime, as long as tracing is not enabled. I.e. as long as tracing is not enabled, this
    // method will not be called and the JVM won't try to load + initialize `GlobalTracer`.
    Tracer tracer = GlobalTracer.get();
    if (tracer != null) {
      httpClient.register(
          (RequestFilter)
              context -> {
                Span span = tracer.activeSpan();
                if (span != null) {
                  Span inner = tracer.buildSpan("Nessie-HTTP").start();
                  Scope scope = tracer.activateSpan(inner);
                  context.addResponseCallback(
                      (responseContext, exception) -> {
                        if (responseContext != null) {
                          try {
                            inner.setTag(
                                "http.status_code", responseContext.getResponseCode().getCode());
                          } catch (IOException e) {
                            // There's not much we can (and probably should) do here.
                          }
                        }
                        if (exception != null) {
                          Map<String, String> log = new HashMap<>();
                          log.put(Fields.EVENT, Tags.ERROR.getKey());
                          log.put(Fields.ERROR_OBJECT, exception.toString());
                          Tags.ERROR.set(inner.log(log), true);
                        }
                        scope.close();
                      });

                  inner
                      .setTag("http.uri", context.getUri().toString())
                      .setTag("http.method", context.getMethod().name());

                  HashMap<String, String> headerMap = new HashMap<>();
                  TextMap httpHeadersCarrier = new TextMapAdapter(headerMap);
                  tracer.inject(inner.context(), Builtin.HTTP_HEADERS, httpHeadersCarrier);
                  headerMap.forEach(context::putHeader);
                }
              });
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T wrap(Class<T> iface, T delegate) {
    return (T)
        Proxy.newProxyInstance(
            delegate.getClass().getClassLoader(),
            new Class[] {iface},
            new ExceptionRewriter(delegate));
  }

  /**
   * This will rewrite exceptions so they are correctly thrown by the api classes. (since the filter
   * will cause them to be wrapped in {@link javax.ws.rs.client.ResponseProcessingException})
   */
  private static class ExceptionRewriter implements InvocationHandler {

    private final Object delegate;

    public ExceptionRewriter(Object delegate) {
      this.delegate = delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException ex) {
        Throwable targetException = ex.getTargetException();
        if (targetException instanceof HttpClientException) {
          if (targetException.getCause() instanceof NessieNotFoundException) {
            throw targetException.getCause();
          }
          if (targetException.getCause() instanceof NessieConflictException) {
            throw targetException.getCause();
          }
        }

        if (targetException instanceof RuntimeException) {
          throw targetException;
        }

        throw ex;
      }
    }
  }

  public URI getUri() {
    return client.getBaseUri();
  }

  @Override
  public void close() {}
}
