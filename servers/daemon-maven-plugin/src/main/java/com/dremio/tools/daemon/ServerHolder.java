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
package com.dremio.tools.daemon;

import java.util.concurrent.ExecutorService;

public class ServerHolder {
  private static Process daemon;
  private static ExecutorService executor;

  public static Process getDaemon() {
    return daemon;
  }

  public static ExecutorService getExecutor() {
    return executor;
  }

  public static void setDaemon(Process daemon) {
    ServerHolder.daemon = daemon;
  }

  public static void setExecutor(ExecutorService executor) {

    ServerHolder.executor = executor;
  }
}
