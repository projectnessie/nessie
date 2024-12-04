/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.cleanup;

import com.google.common.util.concurrent.RateLimiter;

public interface RateLimit {
  void acquire();

  @SuppressWarnings("UnstableApiUsage")
  static RateLimit create(int ratePerSecond) {
    if (ratePerSecond <= 0) {
      return new RateLimit() {
        @Override
        public void acquire() {}

        @Override
        public String toString() {
          return "unlimited";
        }
      };
    }
    return new RateLimit() {
      final RateLimiter limiter = RateLimiter.create(ratePerSecond);

      @Override
      public void acquire() {
        limiter.acquire();
      }

      @Override
      public String toString() {
        return "up to " + ratePerSecond;
      }
    };
  }
}
