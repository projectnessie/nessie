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

import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.SecurityContext;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.auth.UserService;
import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.backend.BranchController;
import com.dremio.nessie.backend.EntityBackend;
import com.dremio.nessie.jgit.JgitBranchController;
import com.dremio.nessie.jwt.KeyGenerator;
import com.dremio.nessie.model.User;
import com.dremio.nessie.model.VersionedWrapper;
import com.dremio.nessie.server.auth.NessieSecurityContext;
import com.dremio.nessie.server.auth.UserServiceDbBackend;

/**
 * Binder for jersey app.
 */
public class NessieServerBinder extends AbstractBinder {

  private static final Logger logger = LoggerFactory.getLogger(NessieServerBinder.class);

  @Override
  protected void configure() {
    bindFactory(ConfigurationFactory.class).to(ServerConfiguration.class);
    String userServiceClass = new ConfigurationFactory().provide()
                                                        .getAuthenticationConfiguration()
                                                        .getUserServiceClassName();

    bindFactory(KeyGeneratorFactory.class).to(KeyGenerator.class).in(Singleton.class);
    bindFactory(BackendFactory.class).to(Backend.class).in(Singleton.class);

    Class<?> usClazz;
    try {
      usClazz = Class.forName(userServiceClass);
    } catch (ClassNotFoundException e) {
      try {
        usClazz = Class.forName("com.dremio.nessie.server.auth.BasicUserService");
      } catch (ClassNotFoundException classNotFoundException) {
        throw new RuntimeException(classNotFoundException);
      }
    }
    bindFactory(UserServiceDbBackendFactory.class).to(UserServiceDbBackend.class);
    bind(usClazz).to(UserService.class);
    bind(NessieSecurityContext.class).to(SecurityContext.class);
    bindFactory(JGitContainerFactory.class).to(BranchController.class);
  }

  public static class JGitContainerFactory implements Factory<BranchController> {

    private final Backend backend;

    @Inject
    private JGitContainerFactory(Backend backend) {
      this.backend = backend;
    }

    @Override
    public BranchController provide() {
      JgitBranchController jgc = new JgitBranchController(backend);
      return jgc;
    }

    @Override
    public void dispose(BranchController instance) {

    }
  }

  private static class UserServiceDbBackendFactory implements Factory<UserServiceDbBackend> {

    private final Backend backend;

    @Inject
    private UserServiceDbBackendFactory(Backend backend) {
      this.backend = backend;
    }

    @Override
    public UserServiceDbBackend provide() {
      return new UserServiceDbBackend() {
        private final EntityBackend<User> delegate = backend.userBackend();

        @Override
        public VersionedWrapper<User> get(String name) {
          return delegate.get(name);
        }

        @Override
        public List<VersionedWrapper<User>> getAll(boolean includeDeleted) {
          return delegate.getAll(includeDeleted);
        }

        @Override
        public VersionedWrapper<User> update(String name, VersionedWrapper<User> table) {
          return delegate.update(name, table);
        }

        @Override
        public void remove(String name) {
          delegate.remove(name);
        }

        @Override
        public void close() {
        }
      };
    }

    @Override
    public void dispose(UserServiceDbBackend userServiceDbBackend) {

    }
  }

  private static class KeyGeneratorFactory implements Factory<KeyGenerator> {

    private final ServerConfiguration configuration;

    @Inject
    private KeyGeneratorFactory(ServerConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public KeyGenerator provide() {
      try {
        Class<?> clazz = Class.forName(configuration.getAuthenticationConfiguration().getKeyGeneratorClassName());
        return (KeyGenerator) clazz.getConstructor().newInstance();
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }

    }

    @Override
    public void dispose(KeyGenerator instance) {

    }
  }
}
