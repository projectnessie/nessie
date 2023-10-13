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
package org.projectnessie.client.api;

import org.projectnessie.client.api.ns.ClientSideCreateNamespace;
import org.projectnessie.client.api.ns.ClientSideDeleteNamespace;
import org.projectnessie.client.api.ns.ClientSideGetMultipleNamespaces;
import org.projectnessie.client.api.ns.ClientSideGetNamespace;
import org.projectnessie.client.api.ns.ClientSideUpdateNamespace;
import org.projectnessie.model.Reference;

/**
 * Interface for the Nessie V2 API implementation.
 *
 * <p>At the java client level this API uses the same builder classes and model types as API v1,
 * however the behaviour of some API methods is different.
 *
 * <p>Most changes between v1 and v2 exist at the REST level (HTTP).
 */
public interface NessieApiV2 extends NessieApiV1 {

  GetRepositoryConfigBuilder getRepositoryConfig();

  UpdateRepositoryConfigBuilder updateRepositoryConfig();

  /** Delete a branch or a tag. */
  DeleteReferenceBuilder<Reference> deleteReference();

  /** Update a branch or a tag (make it point to an arbitrary commit). */
  AssignReferenceBuilder<Reference> assignReference();

  /**
   * {@inheritDoc}
   *
   * @deprecated Use {@code assignReference().asTag()} instead.
   */
  @Override
  @Deprecated
  AssignTagBuilder assignTag();

  /**
   * {@inheritDoc}
   *
   * @deprecated Use {@code assignReference().asBranch()} instead.
   */
  @Override
  @Deprecated
  AssignBranchBuilder assignBranch();

  /**
   * {@inheritDoc}
   *
   * @deprecated Use {@code deleteReference().asTag()} instead.
   */
  @Override
  @Deprecated
  DeleteTagBuilder deleteTag();

  /**
   * {@inheritDoc}
   *
   * @deprecated Use {@code deleteReference().asBranch()} instead.
   */
  @Override
  @Deprecated
  DeleteBranchBuilder deleteBranch();

  @Override
  @Deprecated
  default GetRefLogBuilder getRefLog() {
    throw new UnsupportedOperationException("Reflog is not supported in API v2");
  }

  @Override
  default GetNamespaceBuilder getNamespace() {
    return new ClientSideGetNamespace(this);
  }

  @Override
  default GetMultipleNamespacesBuilder getMultipleNamespaces() {
    return new ClientSideGetMultipleNamespaces(this);
  }

  @Override
  default CreateNamespaceBuilder createNamespace() {
    return new ClientSideCreateNamespace(this);
  }

  @Override
  default DeleteNamespaceBuilder deleteNamespace() {
    return new ClientSideDeleteNamespace(this);
  }

  @Override
  default UpdateNamespaceBuilder updateProperties() {
    return new ClientSideUpdateNamespace(this);
  }

  /**
   * Retrieve the recorded recent history of a reference.
   *
   * <p>A reference's history is a size and time limited record of changes of the reference's
   * current pointer, aka HEAD. The size and time limits are configured in the Nessie server
   * configuration.
   */
  ReferenceHistoryBuilder referenceHistory();
}
