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
 * Marker interface for object types that represent a table's per-reference state.
 *
 * @param <C> the complete type that holds both the global-state and the on-reference-state
 * @param <G> the corresponding global-state type
 */
public interface OnReferenceState<G extends Contents, C extends Contents> {
  C asCompleteState(G state);
}
