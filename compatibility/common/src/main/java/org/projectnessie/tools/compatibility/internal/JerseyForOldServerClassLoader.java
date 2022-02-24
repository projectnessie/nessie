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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import org.projectnessie.tools.compatibility.api.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * "Special" class loader that loads all classes from {@value #CLASS_PREFIX} using the application
 * class loader.
 *
 * <p>This "special class loader" is required to have the code from {@link
 * org.projectnessie.tools.compatibility.jersey.JerseyServer} and related classes available in the
 * class loader for old Nessie server versions.
 */
final class JerseyForOldServerClassLoader extends ClassLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(JerseyForOldServerClassLoader.class);

  static final String CLASS_PREFIX = "org.projectnessie.tools.compatibility.jersey.";
  private final ClassLoader currentVersionClassLoader;
  private final Version version;

  JerseyForOldServerClassLoader(
      Version version, ClassLoader parent, ClassLoader currentVersionClassLoader) {
    super(parent);
    this.version = version;
    this.currentVersionClassLoader = currentVersionClassLoader;
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    if (!name.startsWith(CLASS_PREFIX)) {
      return super.findClass(name);
    }

    String path = name.replace('.', '/') + ".class";

    URL url = currentVersionClassLoader.getResource(path);
    if (url == null) {
      throw new ClassNotFoundException(name);
    }

    LOGGER.debug("Loading class {}' for Nessie server version '{}'", name, version);

    try (InputStream in = url.openConnection().getInputStream()) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[4096];
      int rd;
      while ((rd = in.read(buf)) >= 0) {
        out.write(buf, 0, rd);
      }

      byte[] bytes = out.toByteArray();
      return defineClass(name, bytes, 0, bytes.length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
