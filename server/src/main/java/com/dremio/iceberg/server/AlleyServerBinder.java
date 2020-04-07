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

import javax.inject.Inject;
import javax.ws.rs.core.SecurityContext;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import com.dremio.iceberg.auth.UserService;
import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.backend.EntityBackend;
import com.dremio.iceberg.model.User;
import com.dremio.iceberg.server.auth.AlleySecurityContext;
import com.dremio.iceberg.server.auth.BasicKeyGenerator;
import com.dremio.iceberg.server.auth.KeyGenerator;
import com.dremio.iceberg.server.auth.UserServiceDbBackend;

public class AlleyServerBinder extends AbstractBinder {
  @Override
  protected void configure() {
    bindFactory(ServerConfigurationImpl.ConfigurationFactory.class).to(ServerConfiguration.class);
    String userServiceClass = new ServerConfigurationImpl.ConfigurationFactory().provide().
      getAuthenticationConfiguration().getUserServiceClassName();

    bind(BasicKeyGenerator.class).to(KeyGenerator.class);
    bindFactory(BackendFactory.class).to(Backend.class);

    Class<?> usClazz;
    try {
      usClazz = Class.forName(userServiceClass);
    } catch (ClassNotFoundException e) {
      try {
        usClazz = Class.forName("com.dremio.iceberg.server.auth.BasicUserService");
      } catch (ClassNotFoundException classNotFoundException) {
        throw new RuntimeException(classNotFoundException);
      }
    }
    bindFactory(UserServiceDbBackendFactory.class).to(UserServiceDbBackend.class);
    bind(usClazz).to(UserService.class);
    bind(AlleySecurityContext.class).to(SecurityContext.class);
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
        public User get(String name) {
          return delegate.get(name);
        }

        @Override
        public List<User> getAll(boolean includeDeleted) {
          return delegate.getAll(includeDeleted);
        }

        @Override
        public void create(String name, User table) {
          delegate.create(name, table);
        }

        @Override
        public void update(String name, User table) {
          delegate.update(name, table);
        }

        @Override
        public void remove(String name) {
          delegate.remove(name);
        }
      };
    }

    @Override
    public void dispose(UserServiceDbBackend userServiceDbBackend) {

    }
  }
}
