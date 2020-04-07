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

import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * API represnetation of a User of Iceberg Alley
 */
public final class User implements Base {

  private final String id;
  private final String password;
  private final long createMillis;
  private final boolean active;
  private final String email;
  private final Set<String> roles;
  private final Long version;
  private final long updateMillis;

  public User() {
    this(null,
      null,
      0L,
      true,
      null,
      Sets.newHashSet(),
      null,
      0L);
  }

  public User(
    String id,
    String password,
    long createMillis,
    boolean active,
    String email,
    Set<String> roles,
    Long version,
    long updateMillis
  ) {
    this.id = id;
    this.password = password;
    this.createMillis = createMillis;
    this.active = active;
    this.email = email;
    this.roles = roles;
    this.version = version;
    this.updateMillis = updateMillis;
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

  public OptionalLong getVersion() {
    return version == null ? OptionalLong.empty() : OptionalLong.of(version);
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
    return active == user.active &&
      Objects.equals(id, user.id) &&
      Objects.equals(roles, user.roles);
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
      .add("version=" + version)
      .add("updateMillis=" + updateMillis)
      .toString();
  }

  @JsonIgnore
  public User withUpdateTime(long updateMillis) {
    return new User(
      id,
      password,
      createMillis,
      active,
      email,
      roles,
      version,
      updateMillis
    );
  }

  @JsonIgnore
  public User withPassword(String hashedPassword) {
    return new User(
      id,
      hashedPassword,
      createMillis,
      active,
      email,
      roles,
      version,
      updateMillis
    );
  }

  @JsonIgnore
  public User incrementVersion() {
    return new User(
      id,
      password,
      createMillis,
      active,
      email,
      roles,
      version == null ? 1 : version+1,
      updateMillis
    );
  }
}
