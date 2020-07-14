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

package com.dremio.nessie.serverless;

import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.backend.dynamodb.DynamoDbBackend;
import com.dremio.nessie.server.ServerConfiguration;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Produces;

/**
 * Factory to generate backend based on server config.
 */
@Dependent
public class BackendProducer {

  /**
   * this.
   */
  @Produces
  public Backend producer(ServerConfiguration configuration) {
    return new DynamoDbBackend.BackendFactory().create(configuration);
  }

}
