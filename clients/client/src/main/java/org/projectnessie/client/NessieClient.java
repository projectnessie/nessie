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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_PASSWORD;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URL;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_USERNAME;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Objects;
import java.util.function.Function;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format.Builtin;
import io.opentracing.propagation.TextMap;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.util.GlobalTracer;

public class NessieClient implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(NessieClient.class);

  private final HttpClient client;
  private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
                                                        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  public enum AuthType {
    AWS,
    BASIC,
    NONE
  }

  private final TreeApi tree;
  private final ConfigApi config;
  private final ContentsApi contents;

  /**
   * create new nessie client. All REST api endpoints are mapped here. This client should support any jaxrs implementation
   *
   * @param authType authentication type (AWS, NONE, BASIC)
   * @param path URL for the nessie client (eg http://localhost:19120/api/v1)
   * @param username username (only for BASIC auth)
   * @param password password (only for BASIC auth)
   */
  public NessieClient(AuthType authType, String path, String username, String password) {
    client = HttpClient.builder().setBaseUri(path).setObjectMapper(mapper).build();
    maybeAddTracing(client);
    authFilter(client, authType, username, password);
    client.register(new NessieHttpResponseFilter(mapper));
    contents = wrap(ContentsApi.class, new ClientContentsApi(client));
    tree = wrap(TreeApi.class, new ClientTreeApi(client));
    config = wrap(ConfigApi.class, new ClientConfigApi(client));
  }

  private void maybeAddTracing(HttpClient httpClient) {
    // Catch errors that occur when the OpenTracing classes are not available, log a debug
    // message in that case.
    try {
      TracingConfigurer.addTracing(httpClient);
    } catch (Error | Exception e) {
      LOGGER.debug("Not adding OpenTracing/OpenTelemetry, initialization failed", e);
    }
  }

  private static class TracingConfigurer {
    static void addTracing(HttpClient httpClient) {
      Tracer tracer = GlobalTracer.get();
      if (tracer != null) {
        httpClient.register((RequestFilter) context -> {
          Span span = tracer.activeSpan();
          if (span != null) {
            HashMap<String,String> headerMap = new HashMap<>();
            TextMap httpHeadersCarrier = new TextMapInjectAdapter(headerMap);
            tracer.inject(span.context(), Builtin.HTTP_HEADERS, httpHeadersCarrier);
            headerMap.forEach(context::putHeader);
          }
        });
      }
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
   * (since the filter will cause them to be wrapped in ResposneProcessingException)
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
            throw (NessieNotFoundException) targetException.getCause();
          }
          if (targetException.getCause() instanceof NessieConflictException) {
            throw (NessieConflictException) targetException.getCause();
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

  public static NessieClient basic(String path, String username, String password) {
    return new NessieClient(AuthType.BASIC, path, username, password);
  }

  public static NessieClient aws(String path) {
    return new NessieClient(AuthType.AWS, path, null, null);
  }

  public static NessieClient none(String path) {
    return new NessieClient(AuthType.NONE, path, null, null);
  }

  /**
   * Create a client using a configuration object and standard Nessie configuration keys.
   * @param configuration The function that exploses configuration keys.
   * @return A new Nessie client.
   */
  public static NessieClient withConfig(Function<String, String> configuration) {
    String url = Objects.requireNonNull(configuration.apply(CONF_NESSIE_URL));
    String authType = configuration.apply(CONF_NESSIE_AUTH_TYPE);
    String username = configuration.apply(CONF_NESSIE_USERNAME);
    String password = configuration.apply(CONF_NESSIE_PASSWORD);
    if (authType == null) {
      if (username != null && password != null) {
        authType = AuthType.BASIC.name();
      } else {
        authType = AuthType.NONE.name();
      }
    }
    return new NessieClient(AuthType.valueOf(authType), url, username, password);
  }

}
