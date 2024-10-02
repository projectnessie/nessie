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

/**
 * Thrown when the bloom filter's FPP is above the configured threshold when adding IDs. If this
 * exception is encountered, the current garbage-collection run <em>must</em> be aborted and
 * restarted with a bigger {@link CleanupParams#expectedObjCount()} value.
 */
public class MustRestartWithBiggerFilterException extends Exception {
  public MustRestartWithBiggerFilterException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
