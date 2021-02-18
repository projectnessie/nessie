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
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_TRACING;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URL;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_USERNAME;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format.Builtin;
import io.opentracing.propagation.TextMap;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.util.GlobalTracer;

public class NessieClient implements Closeable {

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
  NessieClient(AuthType authType, String path, String username, String password, boolean enableTracing) {
    HttpClient client = HttpClient.builder().setBaseUri(path).setObjectMapper(mapper).build();
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
          try (Scope scope = tracer.buildSpan("Nessie-HTTP").startActive(true)) {
            HashMap<String, String> headerMap = new HashMap<>();
            TextMap httpHeadersCarrier = new TextMapInjectAdapter(headerMap);
            tracer.inject(scope.span().context(), Builtin.HTTP_HEADERS, httpHeadersCarrier);
            headerMap.forEach(context::putHeader);
          }
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

  /**
   * Create a new {@link Builder} to configure a new {@link NessieClient}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder to configure a new {@link NessieClient}.
   */
  public static class Builder {
    private AuthType authType = AuthType.NONE;
    private String path;
    private String username;
    private String password;
    private boolean tracing;

    /**
     * Same semantics as {@link #fromConfig(Function)}, uses the system properties.
     * @return {@code this}
     * @see #fromConfig(Function)
     */
    public Builder fromSystemProperties() {
      return fromConfig(System::getProperty);
    }

    /**
     * Configure this builder instance using a configuration object and standard Nessie
     * configuration keys defined by the constants defined in {@link NessieConfigConstants}.
     * Non-{@code null} values returned by the {@code configuration}-function will override
     * previously configured values.
     * @param configuration The function that exploses configuration keys.
     * @return {@code this}
     * @see #fromSystemProperties()
     */
    public Builder fromConfig(Function<String, String> configuration) {
      String path = configuration.apply(CONF_NESSIE_URL);
      if (path != null) {
        this.path = path;
      }
      String username = configuration.apply(CONF_NESSIE_USERNAME);
      if (username != null) {
        this.username = username;
      }
      String password = configuration.apply(CONF_NESSIE_PASSWORD);
      if (password != null) {
        this.password = password;
      }
      String authType = configuration.apply(CONF_NESSIE_AUTH_TYPE);
      if (authType != null) {
        this.authType = AuthType.valueOf(authType);
      }
      String tracing = configuration.apply(CONF_NESSIE_TRACING);
      if (tracing != null) {
        this.tracing = Boolean.parseBoolean(tracing);
      }
      return this;
    }

    /**
     * Set the authentication type. Default is {@link AuthType#NONE}.
     * @param authType new auth-type
     * @return {@code this}
     */
    public Builder withAuthType(AuthType authType) {
      this.authType = authType;
      return this;
    }

    /**
     * Set the Nessie server URI. A server URI must be configured.
     * @param path server URI
     * @return {@code this}
     */
    public Builder withPath(String path) {
      this.path = path;
      return this;
    }

    /**
     * Set the username for {@link AuthType#BASIC} authentication.
     * @param username username
     * @return {@code this}
     */
    public Builder withUsername(String username) {
      this.username = username;
      return this;
    }

    /**
     * Set the password for {@link AuthType#BASIC} authentication.
     * @param password password
     * @return {@code this}
     */
    public Builder withPassword(String password) {
      this.password = password;
      return this;
    }

    /**
     * Whether to enable adding the HTTP headers of an active OpenTracing span to all
     * Nessie requests. If enabled, the OpenTracing dependencies must be present at runtime.
     * @param tracing {@code true} to enable passing HTTP headers for active tracing spans.
     * @return {@code this}
     */
    public Builder withTracing(boolean tracing) {
      this.tracing = tracing;
      return this;
    }

    /**
     * Build a new {@link NessieClient}.
     * @return new {@link NessieClient}
     */
    public NessieClient build() {
      return new NessieClient(authType, path, username, password, tracing);
    }
  }
}
