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
package org.projectnessie.services.rest;

import java.util.Objects;
import javax.enterprise.context.RequestScoped;

/**
 * REST API implementations can get the current repo-owner and repo-name from this injectable bean.
 */
@RequestScoped
public class MultiTenant {
  private String owner;
  private String repo;

  /** The repository owner for a multi-tenant request or {@code null}. */
  public String getOwner() {
    return owner;
  }

  /** The repository name for a multi-tenant request or {@code null}. */
  public String getRepo() {
    return repo;
  }

  public boolean isMultiTenant() {
    return owner != null && repo != null;
  }

  void setOwnerAndRepo(String owner, String repo) {
    this.owner = owner;
    this.repo = repo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MultiTenant that = (MultiTenant) o;
    return Objects.equals(owner, that.owner) && Objects.equals(repo, that.repo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(owner, repo);
  }

  @Override
  public String toString() {
    return "MultiTenant{" + "owner='" + owner + '\'' + ", repo='" + repo + '\'' + '}';
  }
}
