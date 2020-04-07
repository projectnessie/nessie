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
package com.dremio.iceberg.auth;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class User implements Principal {
  private final String username;
  private final Set<String> roles;
  private final String password;
  private final long createTime;
  private final boolean active;
  private final Long version;

  public User(String username,
              Set<String> roles,
              String password,
              long createTime,
              boolean active,
              Long version) {
    this.username = username;
    this.roles = roles;
    this.password = password;
    this.createTime = createTime;
    this.active = active;
    this.version = version;
  }

  public User(String username,
              String roles,
              String password,
              long createTime,
              boolean active,
              Long version) {
    this.username = username;
    this.roles = Sets.newHashSet(roles.split(","));
    this.password = password;
    this.createTime = createTime;
    this.active = active;
    this.version = version;
  }

  public User(String username, String roles) {
    this(username, roles, null, 0, true, null);
  }

  public Set<String> getRoles() {
    return roles;
  }

  public String getPassword() {
    return password;
  }

  public long getCreateTime() {
    return createTime;
  }

  public boolean isActive() {
    return active;
  }

  @Override
  public String getName() {
    return username;
  }

  public boolean isInRoles(String role) {
     return roles.contains(role);
  }

  public Long getVersion() {
    return version;
  }

  public User incrementVersion() {
    Long newVersion = version+1;
    return new User(
      username,
      roles,
      password,
      createTime,
      active,
      newVersion
    );
  }
}
