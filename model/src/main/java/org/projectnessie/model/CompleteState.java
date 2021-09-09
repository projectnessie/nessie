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
package org.projectnessie.model;

/**
 * Base interface for {@link Contents} objects that represent both the global and per-reference
 * states. {@link CompleteState} type objects are only returned via the "get-contents" methods of
 * the Nessie-API..
 *
 * @param <G> {@link GlobalState} object type
 * @param <R> {@link OnReferenceState} object type
 */
public interface CompleteState<G extends Contents, R extends Contents> {
  R asRefState();

  G asGlobalState();
}
