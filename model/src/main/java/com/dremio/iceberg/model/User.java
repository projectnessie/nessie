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

package com.dremio.iceberg.model;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * API represnetation of a User of Iceberg Alley.
 */
public final class User implements Base {

  private final String id;
  private final String password;
  private final long createMillis;
  private final boolean active;
  private final String email;
  private final Set<String> roles;
  private final long updateMillis;

  public User() {
    this(null,
         null,
         0L,
         true,
         null,
         Sets.newHashSet(),
         0L);
  }

  public User(String id,
              String password,
              long createMillis,
              boolean active,
              String email,
              Set<String> roles,
              long updateMillis
  ) {
    this.id = id;
    this.password = password;
    this.createMillis = createMillis;
    this.active = active;
    this.email = email;
    this.roles = roles;
    this.updateMillis = updateMillis;
  }

  public static UserBuilder builder() {
    return new UserBuilder();
  }

  public static UserBuilder copyOf(User user) {
    return new UserBuilder(user);
  }

  public String getPassword() {
    return password;
  }

  public long getCreateMillis() {
    return createMillis;
  }

  public boolean isActive() {
    return active;
  }

  public String getEmail() {
    return email;
  }

  public Set<String> getRoles() {
    return ImmutableSet.copyOf(roles);
  }

  @Override
  public String getId() {
    return id;
  }

  public long getUpdateMillis() {
    return updateMillis;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    User user = (User) o;
    return active == user.active && Objects.equals(id, user.id) && Objects.equals(roles,
                                                                                  user.roles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, active, roles);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", User.class.getSimpleName() + "[", "]")
      .add("username='" + id + "'")
      .add("createMillis=" + createMillis)
      .add("active=" + active)
      .add("email='" + email + "'")
      .add("roles=" + roles)
      .add("updateMillis=" + updateMillis)
      .toString();
  }

  /**
   * builder for User object.
   */
  public static class UserBuilder {

    private String id;
    private String password;
    private long createMillis;
    private boolean active;
    private String email;
    private Set<String> roles;
    private long updateMillis;

    UserBuilder() {
    }

    UserBuilder(User user) {
      this.id = user.id;
      this.password = user.password;
      this.createMillis = user.createMillis;
      this.active = user.active;
      this.email = user.email;
      this.roles = user.roles;
      this.updateMillis = user.updateMillis;
    }

    public UserBuilder id(String id) {
      this.id = id;
      return this;
    }

    public UserBuilder password(String password) {
      this.password = password;
      return this;
    }

    public UserBuilder createMillis(long createMillis) {
      this.createMillis = createMillis;
      return this;
    }

    public UserBuilder active(boolean active) {
      this.active = active;
      return this;
    }

    public UserBuilder email(String email) {
      this.email = email;
      return this;
    }

    public UserBuilder roles(Set<String> roles) {
      this.roles = roles;
      return this;
    }

    public UserBuilder updateMillis(long updateMillis) {
      this.updateMillis = updateMillis;
      return this;
    }

    public User build() {
      return new User(id, password, createMillis, active, email, roles, updateMillis);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", UserBuilder.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("password='" + password + "'")
        .add("createMillis=" + createMillis)
        .add("active=" + active)
        .add("email='" + email + "'")
        .add("roles=" + roles)
        .add("updateMillis=" + updateMillis)
        .toString();
    }
  }
}
