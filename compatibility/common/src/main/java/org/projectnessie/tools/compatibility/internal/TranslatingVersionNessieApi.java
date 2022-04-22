/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tools.compatibility.internal;

import static org.projectnessie.tools.compatibility.internal.Util.throwUnchecked;

import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieError;

/**
 * Translates between the current and old Nessie version API and model.
 *
 * <p>Invocations on interfaces are translated using Java {@link Proxy}, model classes are
 * re-serialized using Jackson from within the old Nessie version's class loader and the current
 * (application) class loader.
 */
final class TranslatingVersionNessieApi implements AutoCloseable {

  private final AutoCloseable oldVersionApiInstance;
  private final IdentityHashMap<ClassLoader, Object> objectMappers = new IdentityHashMap<>();
  private final ClassLoader oldVersionClassLoader;
  private final NessieApi proxy;

  TranslatingVersionNessieApi(
      AutoCloseable oldVersionApiInstance,
      Class<? extends NessieApi> currentVersionApiType,
      ClassLoader oldVersionClassLoader) {
    this.oldVersionApiInstance = oldVersionApiInstance;
    this.oldVersionClassLoader = oldVersionClassLoader;
    this.proxy =
        createProxy(
            oldVersionApiInstance,
            Thread.currentThread().getContextClassLoader(),
            oldVersionClassLoader,
            currentVersionApiType);
  }

  @Override
  public void close() throws Exception {
    oldVersionApiInstance.close();
  }

  NessieApi getNessieApi() {
    return proxy;
  }

  @VisibleForTesting
  AutoCloseable getOldVersionApiInstance() {
    return oldVersionApiInstance;
  }

  @VisibleForTesting
  Object[] translateArgs(
      Object[] args, ClassLoader targetClassLoader, ClassLoader reverseClassLoader) {
    if (args == null) {
      return null;
    }
    Object[] translated = new Object[args.length];
    for (int i = 0; i < args.length; i++) {
      translated[i] = translateObject(args[i], targetClassLoader, reverseClassLoader);
    }
    return translated;
  }

  @VisibleForTesting
  Throwable translateException(Throwable e) {
    int status = 0;
    String reason = "<unknown>";

    // Figure out the current Nessie version class name and correct HTTP status code.
    // Old Nessie versions do not have a corresponding ErrorCode for all exception classes.
    String exceptionClassName = e.getClass().getName();
    if (!exceptionClassName.startsWith("org.projectnessie.error.")) {
      switch (exceptionClassName) {
        case "org.projectnessie.client.rest.NessieBadRequestException":
          status = 400;
          exceptionClassName = "org.projectnessie.error.NessieBadRequestException";
          break;
        case "org.projectnessie.client.rest.NessieBackendThrottledException":
          status = 400;
          exceptionClassName = "org.projectnessie.error.NessieBackendThrottledException";
          break;
        case "org.projectnessie.client.rest.NessieForbiddenException":
          status = 401;
          exceptionClassName = "org.projectnessie.error.NessieForbiddenException";
          break;
        case "org.projectnessie.client.rest.NessieInternalServerException":
          status = 500;
          break;
        case "org.projectnessie.client.rest.NessieNotAuthorizedException":
          break;
        default:
          return e;
      }
    }

    try {
      Class<? extends Throwable> testExceptionClass =
          Thread.currentThread()
              .getContextClassLoader()
              .loadClass(exceptionClassName)
              .asSubclass(Throwable.class);
      Class<?> exceptionClass = e.getClass();

      ImmutableNessieError.Builder builder =
          ImmutableNessieError.builder().message(e.getMessage()).status(status).reason(reason);

      // Try to get status code + reason + server-stack-trace + translated ErrorCode enum
      try {
        builder.status((int) exceptionClass.getMethod("getStatus").invoke(e));
      } catch (Exception ignore) {
        //
      }
      try {
        builder.reason((String) exceptionClass.getMethod("getReason").invoke(e));
      } catch (Exception ignore) {
        //
      }
      try {
        builder.serverStackTrace(
            (String) exceptionClass.getMethod("getServerStackTrace").invoke(e));
      } catch (Exception ignore) {
        //
      }
      try {
        Object oldErrorCode = exceptionClass.getMethod("getErrorCode").invoke(e);
        String oldErrorCodeName = ((Enum<?>) oldErrorCode).name();
        builder.errorCode(ErrorCode.valueOf(oldErrorCodeName));
      } catch (Exception ignore) {
        //
      }

      // Try to get the old NessieError instance, prefer the values from NessieError.
      try {
        Object oldNessieError = exceptionClass.getMethod("getError").invoke(e);
        Class<?> oldNessieErrorClass = oldNessieError.getClass();
        builder.status((int) oldNessieErrorClass.getMethod("getStatus").invoke(oldNessieError));
        builder.reason((String) oldNessieErrorClass.getMethod("getReason").invoke(oldNessieError));
        builder.message(
            (String) oldNessieErrorClass.getMethod("getMessage").invoke(oldNessieError));
        builder.serverStackTrace(
            (String) oldNessieErrorClass.getMethod("getServerStackTrace").invoke(oldNessieError));
        Object oldErrorCode = oldNessieErrorClass.getMethod("getErrorCode").invoke(oldNessieError);
        String oldErrorCodeName = ((Enum<?>) oldErrorCode).name();
        builder.errorCode(ErrorCode.valueOf(oldErrorCodeName));
      } catch (Exception ignore) {
        //
      }

      try {
        // Try to construct a current Nessie exception class using NessieError.
        Throwable t =
            testExceptionClass.getConstructor(NessieError.class).newInstance(builder.build());
        t.setStackTrace(e.getStackTrace());
        return t;
      } catch (NoSuchMethodException nse) {
        // Fall back to "standard" constructor, if there's no c'tor taking NessieError.
        return testExceptionClass
            .getConstructor(String.class, Throwable.class)
            .newInstance(e.getMessage(), e);
      }
    } catch (Exception ex) {
      return e;
    }
  }

  @VisibleForTesting
  Object translateObject(Object o, ClassLoader classLoader, ClassLoader reverseClassLoader) {
    if (o instanceof Map) {
      return ((Map<?, ?>) o)
          .entrySet().stream()
              .collect(
                  Collectors.toMap(
                      e -> translateObject(e.getKey(), classLoader, reverseClassLoader),
                      e -> translateObject(e.getValue(), classLoader, reverseClassLoader)));
    }
    if (o instanceof List) {
      return ((List<?>) o)
          .stream()
              .map(e -> translateObject(e, classLoader, reverseClassLoader))
              .collect(Collectors.toList());
    }
    if (o instanceof Set) {
      return ((Set<?>) o)
          .stream()
              .map(e -> translateObject(e, classLoader, reverseClassLoader))
              .collect(Collectors.toSet());
    }

    if (requiresProxy(o)) {
      return createProxy(
          o,
          classLoader,
          reverseClassLoader,
          translateTypes(classLoader, o.getClass().getInterfaces()));
    }
    if (requiresReserialization(o)) {
      return reserialize(o);
    }
    return o;
  }

  @VisibleForTesting
  Object reserialize(Object o) {
    try {
      ClassLoader readClassLoader;
      ClassLoader writeClassLoader;
      if (o.getClass().getClassLoader() == oldVersionClassLoader) {
        readClassLoader = Thread.currentThread().getContextClassLoader();
        writeClassLoader = oldVersionClassLoader;
      } else {
        writeClassLoader = Thread.currentThread().getContextClassLoader();
        readClassLoader = oldVersionClassLoader;
      }

      String serialized = serializeWith(writeClassLoader, o);
      return deserializeWith(readClassLoader, serialized, o.getClass().getName());
    } catch (Exception e) {
      throw throwUnchecked(e);
    }
  }

  @VisibleForTesting
  String serializeWith(ClassLoader classLoader, Object o) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(classLoader);

      Object objectMapper = jacksonObjectMapper(classLoader);

      return (String)
          objectMapper
              .getClass()
              .getMethod("writeValueAsString", Object.class)
              .invoke(objectMapper, o);

    } catch (Exception e) {
      throw throwUnchecked(e);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  @VisibleForTesting
  Object deserializeWith(ClassLoader classLoader, String str, String typeName) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(classLoader);

      Object objectMapper = jacksonObjectMapper(classLoader);

      Object reader = objectMapper.getClass().getMethod("reader").invoke(objectMapper);

      Class<?> type = classLoader.loadClass(typeName);
      String simpleName = type.getSimpleName();
      if (simpleName.startsWith("Immutable")) {
        Class<?>[] ifaces = type.getInterfaces();
        if (ifaces.length == 1) {
          typeName = ifaces[0].getName();
        } else if (type.getSuperclass() != Object.class) {
          typeName = type.getSuperclass().getName();
        } else {
          typeName =
              typeName.substring(0, typeName.lastIndexOf('.') + 1)
                  + simpleName.substring("Immutable".length());
        }
      }
      type = classLoader.loadClass(typeName);

      return reader
          .getClass()
          .getMethod("readValue", String.class, Class.class)
          .invoke(reader, str, type);

    } catch (Exception e) {
      throw throwUnchecked(e);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  private Object jacksonObjectMapper(ClassLoader classLoader) {
    return objectMappers.computeIfAbsent(
        classLoader,
        cl -> {
          try {
            Class<?> classObjectMapper =
                classLoader.loadClass("com.fasterxml.jackson.databind.ObjectMapper");

            return classObjectMapper.getConstructor().newInstance();
          } catch (Exception e) {
            throw throwUnchecked(e);
          }
        });
  }

  @VisibleForTesting
  Class<?>[] translateTypes(ClassLoader classLoader, Class<?>[] types) {
    if (types == null) {
      return null;
    }
    Class<?>[] result = new Class<?>[types.length];
    for (int i = 0; i < types.length; i++) {
      Class<?> type = types[i];
      if (type.getName().startsWith("org.projectnessie.")) {
        try {
          result[i] = classLoader.loadClass(type.getName());
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      } else {
        result[i] = type;
      }
    }
    return result;
  }

  @VisibleForTesting
  static boolean requiresProxy(Object o) {
    if (o == null) {
      return false;
    }
    return o.getClass().getName().startsWith("org.projectnessie.client.");
  }

  @VisibleForTesting
  static boolean requiresReserialization(Object o) {
    if (o == null) {
      return false;
    }
    return o.getClass().getName().startsWith("org.projectnessie.model.");
  }

  @SuppressWarnings("TypeParameterUnusedInFormals")
  private <T> T createProxy(
      Object o, ClassLoader classLoader, ClassLoader reverseClassLoader, Class<?>... interfaces) {
    Object proxy =
        Proxy.newProxyInstance(
            Thread.currentThread().getContextClassLoader(),
            interfaces,
            (proxyInstance, method, args) -> {
              Method targetMethod =
                  o.getClass()
                      .getMethod(
                          method.getName(),
                          translateTypes(reverseClassLoader, method.getParameterTypes()));
              targetMethod.setAccessible(true);
              try {
                Object result =
                    targetMethod.invoke(o, translateArgs(args, classLoader, reverseClassLoader));
                return translateObject(result, classLoader, reverseClassLoader);
              } catch (InvocationTargetException e) {
                throw translateException(e.getTargetException());
              }
            });
    @SuppressWarnings("unchecked")
    T target = (T) proxy;
    return target;
  }
}
