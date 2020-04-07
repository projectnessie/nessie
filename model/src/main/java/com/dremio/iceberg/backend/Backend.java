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
package com.dremio.iceberg.backend;


import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tag;
import com.dremio.iceberg.model.User;
import com.dremio.iceberg.server.ServerConfiguration;

public interface Backend {

  EntityBackend<Table> tableBackend();
  EntityBackend<Tag> tagBackend();
  EntityBackend<User> userBackend();

  interface Factory {
    Backend create(ServerConfiguration config);
  }
}
