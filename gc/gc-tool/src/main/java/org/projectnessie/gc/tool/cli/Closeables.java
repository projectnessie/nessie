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
package org.projectnessie.gc.tool.cli;

import java.util.ArrayList;
import java.util.List;

public final class Closeables implements AutoCloseable {
  private final List<AutoCloseable> autoCloseables = new ArrayList<>();

  @Override
  public void close() throws Exception {
    try {
      Exception ex = null;
      for (int i = autoCloseables.size() - 1; i >= 0; i--) {
        try {
          autoCloseables.get(i).close();
        } catch (Exception e) {
          if (ex == null) {
            ex = e;
          } else {
            ex.addSuppressed(e);
          }
        }
      }
      if (ex != null) {
        throw ex;
      }
    } finally {
      autoCloseables.clear();
    }
  }

  public <T extends AutoCloseable> T add(T autoCloseable) {
    autoCloseables.add(autoCloseable);
    return autoCloseable;
  }

  public <T> T maybeAdd(T maybeAutoCloseable) {
    if (maybeAutoCloseable instanceof AutoCloseable) {
      autoCloseables.add((AutoCloseable) maybeAutoCloseable);
    }
    return maybeAutoCloseable;
  }
}
