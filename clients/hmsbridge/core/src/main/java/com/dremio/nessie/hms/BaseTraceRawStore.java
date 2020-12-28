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
package com.dremio.nessie.hms;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hive.metastore.RawStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A RawStore that will trace all input/output operations to the NessieRawStore. */
public abstract class BaseTraceRawStore implements RawStore {
  // this is done as a physical class rather than solely a proxy because the class name must be
  // provided to Hive via configuration.
  private static final Logger LOG = LoggerFactory.getLogger(BaseTraceRawStore.class);

  private final RawStore delegate;
  private final RawStoreWithRef inner;

  /** Create a tracing raw store. */
  public BaseTraceRawStore() {
    inner = createNewInner();
    delegate =
        (RawStore)
            Proxy.newProxyInstance(
                NessieStoreImpl.class.getClassLoader(),
                new Class[] {RawStore.class},
                new InvocationHandler() {

                  @Override
                  public Object invoke(Object proxy, Method method, Object[] args)
                      throws Throwable {
                    if (inner.getRef() != null) {
                      System.out.print(String.format("[%s] ", inner.getRef()));
                    }

                    if (args == null) {
                      System.out.print(String.format("%s()", method.getName()));
                    } else {
                      System.out.print(
                          String.format(
                              "%s(%s)",
                              method.getName(),
                              args.length == 0
                                  ? ""
                                  : Stream.of(args)
                                      .map(o -> o == null ? null : o.toString())
                                      .collect(Collectors.joining(", "))));
                    }

                    try {
                      boolean isNoReturn =
                          method.getReturnType().equals(void.class)
                              || method.getReturnType().equals(Void.class);
                      Object output = method.invoke(inner, args);
                      if (isNoReturn) {
                        System.out.println(" <no return>");
                      } else {
                        System.out.println(
                            output == null ? " ==> null" : " ==> " + output.toString());
                      }
                      return output;
                    } catch (InvocationTargetException ex) {
                      System.out.println(
                          String.format(
                              " ==> %s: %s",
                              ex.getCause().getClass().getSimpleName(),
                              ex.getCause().getMessage()));
                      throw ex.getCause();
                    }
                  }
                });
  }

  protected abstract RawStoreWithRef createNewInner();
}
