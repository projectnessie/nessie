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
package org.projectnessie.versioned.persist.adapter;

/**
 * Base interface that manages database connections.
 *
 * <p>Concrete implementations serve connections in a database specific way, i.e. by implementing
 * database specific functionality.
 *
 * <p>This interface is used as a common way to let multiple {@link DatabaseAdapter} instances
 *
 * @param <C> {@link DatabaseAdapter} configuration type
 */
public interface DatabaseConnectionProvider<C extends DatabaseConnectionConfig>
    extends AutoCloseable {

  /** Applies connection configuration to this connection provider. */
  void configure(C config);

  /**
   * Initialize the connection provider using the configuration supplied "earlier" via {@link
   * #configure(DatabaseConnectionConfig)}.
   */
  void initialize() throws Exception;

  /**
   * "Forcefully" close all resources held by this provider, even if this instance is still
   * referenced.
   */
  @Override
  void close() throws Exception;
}
