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
package org.projectnessie.test.compatibility;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;

public final class NessieServerHelper {

  private static final String NESSIE_JERSEY_BACKWARDS_PACKAGE =
      "org.projectnessie.test.nessiejersey.";
  private static final String NESSIE_JERSEY_BACKWARDS_COMPATIBLE =
      NESSIE_JERSEY_BACKWARDS_PACKAGE + "NessieJersey";

  private NessieServerHelper() {}

  public static final class NessieServerInstance implements Closeable {
    private final URI uri;
    private final Closeable serverCloseable;

    NessieServerInstance(URI uri, Closeable serverCloseable) {
      this.uri = uri;
      this.serverCloseable = serverCloseable;
    }

    public URI getURI() {
      return uri;
    }

    @Override
    public void close() throws IOException {
      serverCloseable.close();
    }
  }

  public static NessieServerInstance startIsolated(String nessieVersion) {
    try {
      List<Artifact> artifacts =
          DependencyResolver.resolve(
              new DefaultArtifact(
                  "org.projectnessie", "nessie-jaxrs-testextension", "jar", nessieVersion));
      URL[] classpathUrls =
          artifacts.stream()
              .map(Artifact::getFile)
              .map(File::toURI)
              .map(
                  u -> {
                    try {
                      return u.toURL();
                    } catch (MalformedURLException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .toArray(URL[]::new);

      ClassLoader parent = Thread.currentThread().getContextClassLoader();

      ClassLoader cl =
          new URLClassLoader(classpathUrls, parent) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
              if (name.startsWith(NESSIE_JERSEY_BACKWARDS_PACKAGE)) {
                try {
                  URL nessieJerseyUrl = parent.getResource(name.replace('.', '/') + ".class");
                  try (InputStream data = nessieJerseyUrl.openConnection().getInputStream()) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    int rd;
                    byte[] buf = new byte[4096];
                    while ((rd = data.read(buf)) >= 0) {
                      out.write(buf, 0, rd);
                    }
                    byte[] classBytes = out.toByteArray();
                    Class<?> clazz = defineClass(name, classBytes, 0, classBytes.length);
                    if (resolve) {
                      resolveClass(clazz);
                    }
                    return clazz;
                  }
                } catch (Exception e) {
                  throw new ClassNotFoundException(name, e);
                }
              }
              return super.loadClass(name, resolve);
            }
          };
      try {
        Thread.currentThread().setContextClassLoader(cl);

        Class<?> classNessieJersey;
        try {
          // NessieJersey in org.projectnessie.jaxrs.ext is the native implementation shared
          // with all "pure" nessie-jaxrs based tests.
          classNessieJersey = cl.loadClass("org.projectnessie.jaxrs.ext.NessieJersey");
        } catch (ClassNotFoundException e) {
          // NessieJersey in org.projectnessie.test.nessiejersey is a copy of the above, because
          // there was no NessieJersey class before - and it has a "relaxed" behavior when certain
          // resources do not exist (e.g. reflog endpoint only exists since Nessie 0.18).
          classNessieJersey = cl.loadClass(NESSIE_JERSEY_BACKWARDS_COMPATIBLE);
        }
        Constructor<?> ctorNessieJersey = classNessieJersey.getConstructor(ClassLoader.class);
        Object instanceNessieJersey = ctorNessieJersey.newInstance(cl);
        Object uriObject = classNessieJersey.getMethod("getUri").invoke(instanceNessieJersey);
        URI uri = (URI) uriObject;

        return new NessieServerInstance(
            uri,
            () -> {
              try {
                instanceNessieJersey.getClass().getMethod("close").invoke(instanceNessieJersey);
              } catch (IllegalAccessException | NoSuchMethodException e) {
                throw new RuntimeException(e);
              } catch (InvocationTargetException e) {
                if (e.getCause() instanceof IOException) {
                  throw (IOException) e.getCause();
                }
                if (e.getCause() instanceof RuntimeException) {
                  throw (RuntimeException) e.getCause();
                }
                throw new RuntimeException(e);
              }
            });
      } finally {
        Thread.currentThread().setContextClassLoader(parent);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
