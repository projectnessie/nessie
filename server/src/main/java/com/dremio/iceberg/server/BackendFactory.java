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

import com.dremio.iceberg.backend.Backend;
import java.lang.reflect.Constructor;
import javax.inject.Inject;
import org.glassfish.hk2.api.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to generate backend based on server config.
 */
public class BackendFactory implements Factory<Backend> {
  private static final Logger logger = LoggerFactory.getLogger(BackendFactory.class);
  private final ServerConfiguration configuration;

  @Inject
  public BackendFactory(ServerConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Backend provide() {
    Class<?> dbClazz;
    try {
      dbClazz = Class
        .forName(configuration.getDatabaseConfiguration().getDbClassName() + "$BackendFactory");
    } catch (ClassNotFoundException e) {
      try {
        dbClazz = Class.forName("com.dremio.iceberg.backend.simple.InMemory$BackendFactory");

      } catch (ClassNotFoundException classNotFoundException) {
        throw new RuntimeException(classNotFoundException);
      }
    }
    try {
      Constructor<?> constructor = dbClazz.getConstructor();
      Backend.Factory factory = (Backend.Factory) constructor.newInstance();
      return factory.create(configuration);
    } catch (Throwable t) {
      throw new RuntimeException("no suitable constructor for backend found", t);
    }
  }

  @Override
  public void dispose(Backend backend) {
    try {
      backend.close();
    } catch (Exception e) {
      logger.error("failed to close db backend", e);
    }
  }
}
