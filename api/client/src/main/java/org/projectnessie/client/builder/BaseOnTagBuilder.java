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
package org.projectnessie.client.builder;

import org.projectnessie.client.api.OnTagBuilder;

public abstract class BaseOnTagBuilder<R extends OnTagBuilder<R>> implements OnTagBuilder<R> {
  protected String tagName;
  protected String hash;

  @SuppressWarnings("unchecked")
  @Override
  public R tagName(String tagName) {
    this.tagName = tagName;
    return (R) this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public R hash(String hash) {
    this.hash = hash;
    return (R) this;
  }
}
