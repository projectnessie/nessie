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
package org.projectnessie.versioned.storage.commontests.objtypes;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.versioned.storage.common.persist.ObjId;

public interface SimpleTestBean {

  @Nullable
  ObjId parent();

  @Nullable
  String text();

  @Nullable
  Number number();

  @Nullable
  Map<String, String> map();

  @Nullable
  List<String> list();

  @Nullable
  Instant instant();

  Optional<String> optional();
}
