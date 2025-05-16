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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientResponseFactory;
import org.projectnessie.jaxrs.tests.ValidatingApiV1ResponseFactory;
import org.projectnessie.quarkus.tests.profiles.QuarkusTestProfilePersistInmemory;

/**
 * This tests runs the usual API v1 tests, but checks that JSON responses do not have API v2
 * attributes.
 */
@QuarkusTest
@TestProfile(QuarkusTestProfilePersistInmemory.class)
@NessieApiVersions(versions = NessieApiVersion.V1)
@NessieClientResponseFactory(ValidatingApiV1ResponseFactory.class)
public class TestQuarkusWithOlderRestApiV1ClientInMemory extends AbstractQuarkusRest {}
