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
package org.projectnessie.client.rest.v1;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Optional;
import org.projectnessie.api.v1.http.HttpConfigApi;
import org.projectnessie.api.v1.http.HttpContentApi;
import org.projectnessie.api.v1.http.HttpDiffApi;
import org.projectnessie.api.v1.http.HttpNamespaceApi;
import org.projectnessie.api.v1.http.HttpTreeApi;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.BaseNessieClientServerException;

public final class RestV1Client extends NessieApiClient {

  private final HttpClient httpClient;

  /**
   * Create new HTTP Nessie client. All REST api endpoints are mapped here. This client should
   * support any JAX-RS implementation
   */
  public RestV1Client(HttpClient client) {
    super(
        wrap(HttpConfigApi.class, new RestV1ConfigClient(client)),
        wrap(HttpTreeApi.class, new RestV1TreeClient(client)),
        wrap(HttpContentApi.class, new RestV1ContentClient(client)),
        wrap(HttpDiffApi.class, new RestV1DiffClient(client)),
        wrap(HttpNamespaceApi.class, new RestV1NamespaceClient(client)));
    this.httpClient = client;
  }

  @Override
  public void close() {
    this.httpClient.close();
  }

  @Override
  public Optional<HttpClient> httpClient() {
    return Optional.of(httpClient);
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
   * This will rewrite exceptions, so they are correctly thrown by the api classes. (since the
   * filter will cause them to be wrapped in {@link javax.ws.rs.client.ResponseProcessingException})
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
          Throwable cause = targetException.getCause();
          if (cause instanceof BaseNessieClientServerException) {
            throw cause;
          }
        }

        if (targetException instanceof RuntimeException) {
          throw targetException;
        }

        throw ex;
      }
    }
  }
}
