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
package com.dremio.nessie.client;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import javax.ws.rs.client.ResponseProcessingException;

import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl;

import com.dremio.nessie.api.ConfigApi;
import com.dremio.nessie.api.ContentsApi;
import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.client.auth.AuthFilter;
import com.dremio.nessie.client.rest.ObjectMapperContextResolver;
import com.dremio.nessie.client.rest.ResponseCheckFilter;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;

import io.opentracing.contrib.jaxrs2.client.ClientTracingFeature;

public class NessieClient implements Closeable {

  public enum AuthType {
    AWS,
    BASIC,
    NONE
  }

  static {
    System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
  }

  private final ResteasyClient client;
  private final TreeApi tree;
  private final ConfigApi config;
  private final ContentsApi contents;

  /**
   * create new nessie client. All REST api endpoints are mapped here.
   *
   * @param path URL for the nessie client (eg http://localhost:19120/api/v1)
   */
  public NessieClient(AuthType authType, String path, String username, String password) {

    client = new ResteasyClientBuilderImpl().register(ObjectMapperContextResolver.class)
                                            .register(ClientTracingFeature.class)
                                            .register(ResponseCheckFilter.class)
                                            .build();
    ResteasyWebTarget target = client.target(path);
    AuthFilter authFilter = new AuthFilter(authType, username, password, target);
    client.register(authFilter);
    contents = wrap(ContentsApi.class, target.proxy(ContentsApi.class));
    tree = wrap(TreeApi.class, target.proxy(TreeApi.class));
    config = wrap(ConfigApi.class, target.proxy(ConfigApi.class));
  }

  @SuppressWarnings("unchecked")
  private <T> T wrap(Class<T> iface, T delegate) {
    return (T) Proxy.newProxyInstance(delegate.getClass().getClassLoader(), new Class[]{iface}, new ExceptionRewriter(delegate));
  }

  /**
   * This will rewrite exceptions so they are correctly thrown by the api classes
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
        if(targetException instanceof ResponseProcessingException) {
          if(targetException.getCause() instanceof NessieNotFoundException) {
            throw (NessieNotFoundException) targetException.getCause();
          }
          if(targetException.getCause() instanceof NessieConflictException) {
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
    client.close();
  }

  public static NessieClient basic(String path, String username, String password) {
    return new NessieClient(AuthType.BASIC, path, username, password);
  }

  public static NessieClient aws(String path) {
    return new NessieClient(AuthType.AWS, path, null, null);
  }

}
