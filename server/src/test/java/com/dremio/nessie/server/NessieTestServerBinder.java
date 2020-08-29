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

package com.dremio.nessie.server;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.SecurityContext;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import com.dremio.nessie.auth.User;
import com.dremio.nessie.auth.UserService;
import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.backend.simple.InMemory;
import com.dremio.nessie.jgit.JgitBranchControllerLegacy;
import com.dremio.nessie.jwt.KeyGenerator;
import com.dremio.nessie.model.ImmutableUser;
import com.dremio.nessie.server.auth.BasicKeyGenerator;
import com.dremio.nessie.server.auth.NessieSecurityContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class NessieTestServerBinder extends AbstractBinder {

  public static final TestServerConfigurationImpl settings = new TestServerConfigurationImpl();


  public NessieTestServerBinder() {
  }

  @Override
  protected void configure() {
    bindFactory(TestServerConfigurationFactory.class).to(ServerConfiguration.class);
    bindFactory(BackendFactory.class).to(Backend.class);
    bind(NessieSecurityContext.class).to(SecurityContext.class);
    bind(TestUserService.class).to(UserService.class);
    bind(BasicKeyGenerator.class).to(KeyGenerator.class);
    bindFactory(JGitContainerFactory.class).to(JgitBranchControllerLegacy.class);
  }

  public static class JGitContainerFactory implements Factory<JgitBranchControllerLegacy> {
    private static boolean enabled = false;

    private final Backend backend;

    @Inject
    private JGitContainerFactory(Backend backend) {
      this.backend = backend;
    }

    @Override
    public JgitBranchControllerLegacy provide() {
      JgitBranchControllerLegacy jgc = new JgitBranchControllerLegacy(backend);
      if (enabled) {
        return jgc;
      }
      try {
        jgc.create("master", null, null, null);
        enabled = true;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return jgc;
    }

    @Override
    public void dispose(JgitBranchControllerLegacy instance) {

    }
  }

  public static final class BackendFactory implements Factory<Backend> {
    private static final Backend backend = new InMemory();

    @Override
    public Backend provide() {
      return backend;
    }

    @Override
    public void dispose(Backend instance) {
      try {
        backend.close();
      } catch (Exception e) {
        //pass
      }
    }
  }

  public static class TestUserService implements UserService {

    private static final Map<String, User> userMap = new HashMap<>();

    static {
      userMap.put("normal", new User(getUser(false)));
      userMap.put("test", new User(getUser(true)));
    }

    @Override
    public String authorize(String login, String password) {
      if (userMap.containsKey(login)) {
        return (login.equals("normal")) ? "normal" : "ok";
      }
      throw new NotAuthorizedException("wrong username/password");
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
      com.dremio.nessie.model.User modelUser = NessieTestServerBinder.unwrap(user);
      userMap.put(user.getName(), new User(modelUser));
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

  private static com.dremio.nessie.model.User getUser(boolean admin) {
    return ImmutableUser.builder()
                        .id(admin ? "test" : "normal")
                        .password("")
                        .addAllRoles(
                          admin ? ImmutableSet.of("admin", "user") : ImmutableSet.of("user"))
                        .build();
  }

  static com.dremio.nessie.model.User unwrap(User user) {
    try {
      Method method = User.class.getDeclaredMethod("unwrap");
      method.setAccessible(true);
      return (com.dremio.nessie.model.User) method.invoke(user);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
}
