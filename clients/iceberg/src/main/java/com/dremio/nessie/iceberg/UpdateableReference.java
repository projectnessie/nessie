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
package com.dremio.nessie.iceberg;

import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.iceberg.exceptions.NotFoundException;

import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.Hash;
import com.dremio.nessie.model.Reference;

class UpdateableReference {

  private Reference reference;
  private final TreeApi client;

  public UpdateableReference(Reference reference, TreeApi client) {
    super();
    this.reference = reference;
    this.client = client;
  }

  public boolean refresh() {
    if (reference instanceof Hash) {
      return false;
    }
    Reference oldReference = reference;
    try {
      reference = client.getReferenceByName(reference.getName());
    } catch (NessieNotFoundException e) {
      throw new NotFoundException("Failure refreshing data, table no longer exists.", e);
    }
    return !oldReference.equals(reference);
  }

  public boolean isBranch() {
    return reference instanceof Branch;
  }

  public UpdateableReference clone() {
    return new UpdateableReference(reference, client);
  }

  public String getHash() {
    return reference.getHash();
  }

  public Branch getAsBranch() {
    Preconditions.checkArgument(isBranch());
    return (Branch) reference;
  }

  public void checkMutable() {
    if (!isBranch()) {
      throw new IllegalArgumentException("You can only mutate tables when using a branch.");
    }
  }
}
