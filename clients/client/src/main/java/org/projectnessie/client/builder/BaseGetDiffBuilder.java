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

import org.projectnessie.client.api.GetDiffBuilder;

public abstract class BaseGetDiffBuilder implements GetDiffBuilder {

  protected String fromRefName;
  protected String fromHashOnRef;
  protected String toRefName;
  protected String toHashOnRef;

  @Override
  public GetDiffBuilder fromRefName(String fromRefName) {
    this.fromRefName = fromRefName;
    return this;
  }

  @Override
  public GetDiffBuilder fromHashOnRef(String fromHashOnRef) {
    this.fromHashOnRef = fromHashOnRef;
    return this;
  }

  @Override
  public GetDiffBuilder toRefName(String toRefName) {
    this.toRefName = toRefName;
    return this;
  }

  @Override
  public GetDiffBuilder toHashOnRef(String toHashOnRef) {
    this.toHashOnRef = toHashOnRef;
    return this;
  }
}
