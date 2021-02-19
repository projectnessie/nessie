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
package org.projectnessie.client;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.projectnessie.api.ConfigApi;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.auth.AwsAuth;
import org.projectnessie.client.auth.BasicAuthFilter;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.RequestFilter;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.propagation.Format.Builtin;
import io.opentracing.propagation.TextMap;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;

class NessieHttpClient implements NessieClient {

  private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
                                                        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  private final TreeApi tree;
  private final ConfigApi config;
  private final ContentsApi contents;

  /**
   * Create new HTTP {@link NessieClient}. All REST api endpoints are mapped here. This client should support
   * any jaxrs implementation
   *
   * @param authType authentication type (AWS, NONE, BASIC)
   * @param uri URL for the nessie client (eg http://localhost:19120/api/v1)
   * @param username username (only for BASIC auth)
   * @param password password (only for BASIC auth)
   */
  NessieHttpClient(AuthType authType, URI uri, String username, String password, boolean enableTracing) {
    HttpClient client = HttpClient.builder().setBaseUri(uri).setObjectMapper(mapper).build();
    if (enableTracing) {
      addTracing(client);
    }
    authFilter(client, authType, username, password);
    client.register(new NessieHttpResponseFilter(mapper));
    contents = wrap(ContentsApi.class, new ClientContentsApi(client));
    tree = wrap(TreeApi.class, new ClientTreeApi(client));
    config = wrap(ConfigApi.class, new ClientConfigApi(client));
  }

  private static void addTracing(HttpClient httpClient) {
    // It's safe to reference `GlobalTracer` here even without the required dependencies available
    // at runtime, as long as tracing is not enabled. I.e. as long as tracing is not enabled, this
    // method will not be called and the JVM won't try to load + initialize `GlobalTracer`.
    Tracer tracer = GlobalTracer.get();
    if (tracer != null) {
      httpClient.register((RequestFilter) context -> {
        Span span = tracer.activeSpan();
        if (span != null) {
          Scope scope = tracer.buildSpan("Nessie-HTTP").startActive(true);
          context.addResponseCallback((responseContext, exception) -> {
            if (responseContext != null) {
              try {
                scope.span().setTag("http.status", responseContext.getResponseCode().getCode());
              } catch (IOException e) {
                // There's not much we can (and probably should) do here.
              }
            }
            if (exception != null) {
              Map<String, String> log = new HashMap<>();
              log.put(Fields.EVENT, Tags.ERROR.getKey());
              log.put(Fields.ERROR_OBJECT, exception.toString());
              Tags.ERROR.set(scope.span().log(log), true);
            }
            scope.close();
          });

          scope.span().setTag("http.uri", context.getUri().toString());

          HashMap<String, String> headerMap = new HashMap<>();
          TextMap httpHeadersCarrier = new TextMapInjectAdapter(headerMap);
          tracer.inject(scope.span().context(), Builtin.HTTP_HEADERS, httpHeadersCarrier);
          headerMap.forEach(context::putHeader);
        }
      });
    }
  }

  private void authFilter(HttpClient client, AuthType authType, String username, String password) {
    switch (authType) {
      case AWS:
        client.register(new AwsAuth(mapper));
        break;
      case BASIC:
        client.register(new BasicAuthFilter(username, password));
        break;
      case NONE:
        break;
      default:
        throw new IllegalArgumentException(String.format("Cannot instantiate auth filter for %s. Not a valid auth type", authType));
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T wrap(Class<T> iface, T delegate) {
    return (T) Proxy.newProxyInstance(delegate.getClass().getClassLoader(), new Class[] {iface}, new ExceptionRewriter(delegate));
  }

  /**
   * This will rewrite exceptions so they are correctly thrown by the api classes.
   * (since the filter will cause them to be wrapped in {@link javax.ws.rs.client.ResponseProcessingException})
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


  public TreeApi getTreeApi() {
    return tree;
  }

  public ContentsApi getContentsApi() {
    return contents;
  }

  public ConfigApi getConfigApi() {
    return config;
  }

  @Override
  public void close() {
  }
}
