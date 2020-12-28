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
package com.dremio.nessie.versioned.impl;

import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.impl.InternalRef.Type;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;

class InternalRefId implements HasId {

  private final String name;
  private final Id id;
  private final Type type;

  private InternalRefId(String name, Type type) {
    this.name = name;
    this.id = Id.build(name);
    this.type = type;
  }

  private InternalRefId(Id id, Type type) {
    this.name = null;
    this.type = type;
    this.id = id;
  }

  public static InternalRefId of(Ref ref) throws ReferenceNotFoundException {
    if (ref instanceof BranchName) {
      return InternalRefId.ofBranch(((BranchName) ref).getName());
    } else if (ref instanceof TagName) {
      return InternalRefId.ofTag(((TagName) ref).getName());
    } else if (ref instanceof Hash) {
      return InternalRefId.ofHash((Hash) ref);
    } else {
      throw new IllegalStateException();
    }
  }

  public static InternalRefId ofUnknownName(String name) {
    return new InternalRefId(name, Type.UNKNOWN);
  }

  public static InternalRefId ofTag(String name) {
    return new InternalRefId(name, Type.TAG);
  }

  public static InternalRefId ofBranch(String name) {
    return new InternalRefId(name, Type.BRANCH);
  }

  public static InternalRefId ofHash(Hash hash) throws ReferenceNotFoundException {
    return new InternalRefId(Id.of(hash), Type.HASH);
  }

  public static InternalRefId ofHash(Id id) {
    return new InternalRefId(id, Type.HASH);
  }

  @Override
  public Id getId() {
    return id;
  }

  public Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }
}
