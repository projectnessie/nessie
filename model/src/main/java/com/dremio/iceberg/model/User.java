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

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class User {

  private String username;
  private String password;
  private long createMillis;
  private boolean active;
  private String email;
  private Set<String> roles;
  private Long version;

  @JsonCreator
  public User(
    @JsonProperty("username") String username,
    @JsonProperty("password") String password,
    @JsonProperty("createMillis") long createMillis,
    @JsonProperty("active") boolean active,
    @JsonProperty("email") String email,
    @JsonProperty("roles") Set<String> roles,
    @JsonProperty("version") Long version
  ) {

    this.username = username;
    this.password = password;
    this.createMillis = createMillis;
    this.active = active;
    this.email = email;
    this.roles = roles;
    this.version = version;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public long getCreateMillis() {
    return createMillis;
  }

  public void setCreateMillis(long createMillis) {
    this.createMillis = createMillis;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public void setRoles(Set<String> roles) {
    this.roles = roles;
  }

  public Long getVersion() {
    return version;
  }

  public void setVersion(Long version) {
    this.version = version;
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
    return createMillis == user.createMillis &&
      active == user.active &&
      Objects.equal(username, user.username) &&
      Objects.equal(password, user.password) &&
      Objects.equal(email, user.email) &&
      Objects.equal(version, user.version) &&
      Objects.equal(roles, user.roles);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(username, password, createMillis, active, email, roles, version);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("username", username)
      .add("createMillis", createMillis)
      .add("active", active)
      .add("email", email)
      .add("roles", roles)
      .add("version", version)
      .toString();
  }
}
