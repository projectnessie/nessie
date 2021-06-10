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
package org.projectnessie.versioned.gc;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * convert a value into a stream of asset keys.
 *
 * @param <T> Value type. Each value is assumed to have a set of assets it depends on.
 * @param <R> Concrete type of asset key. eg path on a filesystem or database record.
 */
public interface AssetKeyConverter<T, R extends AssetKey> extends Function<T, Stream<R>> {}
