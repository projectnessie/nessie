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

import org.projectnessie.model.Reference;

public abstract class BaseOnReferenceBuilder<R> {
  protected String refName;
  protected String hashOnRef;
  protected Reference.ReferenceType type;

  public BaseOnReferenceBuilder() {
    this(null);
  }

  public BaseOnReferenceBuilder(Reference.ReferenceType assumedType) {
    this.type = assumedType;
  }

  @SuppressWarnings("unchecked")
  public R branchName(String branchName) {
    this.refName = branchName;
    this.type = Reference.ReferenceType.BRANCH;
    return (R) this;
  }

  @SuppressWarnings("unchecked")
  public R tagName(String tagName) {
    this.refName = tagName;
    this.type = Reference.ReferenceType.TAG;
    return (R) this;
  }

  @SuppressWarnings("unchecked")
  public R refName(String refName) {
    this.refName = refName;
    return (R) this;
  }

  @SuppressWarnings("unchecked")
  public R hashOnRef(String hashOnRef) {
    this.hashOnRef = hashOnRef;
    return (R) this;
  }

  public R hash(String hashOnRef) {
    return hashOnRef(hashOnRef);
  }

  @SuppressWarnings("unchecked")
  public R refType(Reference.ReferenceType referenceType) {
    this.type = referenceType;
    return (R) this;
  }
}
