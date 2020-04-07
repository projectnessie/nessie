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
package com.dremio.iceberg.server.auth;

import java.security.Principal;
import java.util.Set;

import com.google.common.collect.Sets;

public class User implements Principal {
  private final String username;
  private final Set<String> roles;

  public User(String username, String roles) {
    this.username = username;
    this.roles = Sets.newHashSet(roles.split(","));
  }

  @Override
  public String getName() {
    return username;
  }

  public boolean isInRoles(String role) {
     return roles.contains(role);
  }
}
