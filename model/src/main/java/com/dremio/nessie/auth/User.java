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

package com.dremio.nessie.auth;

import java.security.Principal;
import java.util.Objects;
import java.util.StringJoiner;
import javax.security.auth.Subject;

/**
 * Principal which defines what a user can do in Nessie.
 */
public final class User implements Principal {

  private final com.dremio.nessie.model.User user;

  public User(com.dremio.nessie.model.User user) {
    this.user = user;
  }

  @Override
  public String getName() {
    return user.getId();
  }

  public boolean isInRoles(String role) {
    return user.getRoles().contains(role);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", User.class.getSimpleName() + "[", "]")
      .add("user=" + user)
      .toString();
  }

  @Override
  public boolean implies(Subject subject) {
    return subject.getPrincipals().stream().anyMatch(p -> p.equals(this));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    User user1 = (User) o;
    return Objects.equals(user, user1.user);
  }

  @Override
  public int hashCode() {
    return Objects.hash(user);
  }

  public String email() {
    return user.getEmail();
  }

  com.dremio.nessie.model.User unwrap() {
    return user; //todo I don't want to expose this model...how can I prevent it?
  }
}
