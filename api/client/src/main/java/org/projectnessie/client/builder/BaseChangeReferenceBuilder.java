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
package org.projectnessie.client.builder;

import org.projectnessie.client.api.ChangeReferenceBuilder;
import org.projectnessie.model.Reference.ReferenceType;

public abstract class BaseChangeReferenceBuilder<B> implements ChangeReferenceBuilder<B> {
  protected String refName;
  protected String expectedHash;
  protected ReferenceType type;

  @SuppressWarnings("unchecked")
  @Override
  public B refName(String name) {
    this.refName = name;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public B refType(ReferenceType referenceType) {
    this.type = referenceType;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public B hash(String hash) {
    this.expectedHash = hash;
    return (B) this;
  }
}
