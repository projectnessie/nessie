/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.server.catalog.auth;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;

@QuarkusIntegrationTest
@TestProfile(AbstractAuthEnabledTests.Profiles.S3N.class)
public class ITOAuthIcebergCatalogS3N extends AbstractAuthEnabledTests {

  @Override
  protected Map<String, String> catalogOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2);
    options.put(OAuth2Properties.OAUTH2_SERVER_URI, tokenEndpoint.toString());
    options.put(OAuth2Properties.SCOPE, "email");
    options.put(OAuth2Properties.CREDENTIAL, clientId + ":" + clientSecret);
    options.put(OAuth2Properties.TOKEN_REFRESH_ENABLED, "false");
    return options;
  }

  @Override
  protected String scheme() {
    return "s3n";
  }
}
