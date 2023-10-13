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

import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

class OAuth2TokenRefreshExecutor extends ScheduledThreadPoolExecutor {

  OAuth2TokenRefreshExecutor(Duration keepAlive) {
    super(1, new OAuth2TokenRefreshThreadFactory());
    setKeepAliveTime(keepAlive.toNanos(), TimeUnit.NANOSECONDS);
    allowCoreThreadTimeOut(true);
  }

  static class OAuth2TokenRefreshThreadFactory implements ThreadFactory {

    @Override
    public Thread newThread(@Nonnull Runnable r) {
      Thread thread = new Thread(r, "oauth2-token-refresh");
      thread.setDaemon(true);
      return thread;
    }
  }
}
