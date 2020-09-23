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
import java.util.concurrent.Future;

public class ServerHolder {
  private static Future<?> daemon;
  private static ExecutorService executor;
  private static ClassLoader classLoader;

  public static Future<?> getDaemon() {
    return daemon;
  }

  public static ExecutorService getExecutor() {
    return executor;
  }

  public static void setDaemon(Future<?> daemon) {
    ServerHolder.daemon = daemon;
  }

  public static void setExecutor(ExecutorService executor) {
    ServerHolder.executor = executor;
  }

  public static ClassLoader getClassLoader() {
    return classLoader;
  }

  public static void setClassLoader(ClassLoader classLoader) {
    ServerHolder.classLoader = classLoader;
  }
}
