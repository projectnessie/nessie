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
package org.projectnessie.server;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.oidc.server.OidcWiremockTestResource;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

@QuarkusIntegrationTest
@QuarkusTestResource(OidcWiremockTestResource.class)
@TestProfile(value = AbstractOpenIdAuthentication.Profile.class)
@DisabledIfSystemProperty(
    named = "quarkus.container-image.build",
    matches = "true",
    disabledReason =
        "The OidcWiremock resource runs on the Docker host, which is not accessible when the tested "
            + "Nessie server runs in a Docker container.")
public class ITOpenIdAuthentication extends AbstractOpenIdAuthentication {}
