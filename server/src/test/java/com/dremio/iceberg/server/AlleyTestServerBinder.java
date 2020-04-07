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
package com.dremio.iceberg.server;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.SecurityContext;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import com.dremio.iceberg.backend.simple.InMemory;
import com.dremio.iceberg.server.auth.AlleySecurityContext;
import com.dremio.iceberg.server.auth.BasicKeyGenerator;
import com.dremio.iceberg.server.auth.KeyGenerator;
import com.dremio.iceberg.auth.User;
import com.dremio.iceberg.auth.UserService;
import com.dremio.iceberg.backend.Backend;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class AlleyTestServerBinder extends AbstractBinder {
  public static final TestServerConfigurationImpl settings = new TestServerConfigurationImpl();

  @Override
  protected void configure() {
    bindFactory(TestServerConfigurationFactory.class).to(ServerConfiguration.class);
    bind(InMemory.class).to(Backend.class);
    bind(AlleySecurityContext.class).to(SecurityContext.class);
    bind(TestUserService.class).to(UserService.class);
    bind(BasicKeyGenerator.class).to(KeyGenerator.class);
  }

  public static class TestUserService implements UserService {
    private static final Map<String, User> userMap = Maps.newHashMap();
    static {
      userMap.put("normal", new User("normal", "user", null, 0, true, 1L, 0L));
      userMap.put("test", new User("test", "admin,user", null, 0, true, 1L, 0L));
    }

    @Override
    public String authorize(String login, String password) {
      return (login.equals("normal")) ? "normal": "ok";
    }

    @Override
    public User validate(String token) {
      if (token.equals("normal")) {
        return userMap.get("normal");
      }
      return userMap.get("test");
    }

    @Override
    public Optional<User> fetch(String username) {
      if (userMap.containsKey(username)) {
        return Optional.of(userMap.get(username));
      }
      return Optional.empty();
    }

    @Override
    public List<User> fetchAll() {
      return ImmutableList.copyOf(userMap.values());
    }

    @Override
    public void create(User user) {
      userMap.put(user.getName(), user);
    }

    @Override
    public void update(User user) {
      userMap.put(user.getName(), user.incrementVersion());
    }

    @Override
    public void delete(String user) {
      userMap.remove(user);
    }
  }

  private static class TestServerConfigurationFactory implements Factory<ServerConfiguration> {
    @Override
    public ServerConfiguration provide() {
      return settings;
    }

    @Override
    public void dispose(ServerConfiguration serverConfiguration) {

    }
  }
}
