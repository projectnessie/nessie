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

package com.dremio.nessie.client;

import com.dremio.nessie.client.aws.AwsClient;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ImmutableBranch;
import java.util.Arrays;

public final class ClientFactory {

  public enum ClientType {
    BASIC,
    AWS
  }

  private ClientFactory() {

  }

  /**
   * build Client based on client type.
   */
  public static BaseClient get(ClientType type, String endpoint, String username, String password) {
    switch (type) {
      case BASIC:
        return new NessieClient(endpoint, username, password);
      case AWS:
        return new AwsClient(endpoint);
      default:
        throw new UnsupportedOperationException("can't instantiate a client of type " + type);
    }
  }
}
