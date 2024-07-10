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
package org.projectnessie.versioned.transfer;

public enum ProgressEvent {
  FINISHED,
  START_PREPARE,
  END_PREPARE,
  END_FINALIZE,
  FINALIZE_PROGRESS,
  START_FINALIZE,
  START_NAMED_REFERENCES,
  NAMED_REFERENCE_WRITTEN,
  END_NAMED_REFERENCES,
  START_COMMITS,
  COMMIT_WRITTEN,
  END_COMMITS,
  START_GENERIC,
  GENERIC_WRITTEN,
  END_GENERIC,
  START_META,
  END_META,
  STARTED
}
