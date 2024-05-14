/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.client.auth.oauth2;

import java.io.PrintStream;
import java.util.function.Consumer;

/**
 * Emulates a resource owner (user) that "reads" the console (system out) and "uses" their browser
 * to authenticate against an authorization server.
 */
public interface ResourceOwnerEmulator extends AutoCloseable {

  ResourceOwnerEmulator INACTIVE = new ResourceOwnerEmulator() {};

  default PrintStream getConsole() {
    return System.out;
  }

  default void setErrorListener(Consumer<Throwable> callback) {}

  default void setCompletionListener(Runnable listener) {}

  @Override
  default void close() throws Exception {
    // do nothing
  }
}
