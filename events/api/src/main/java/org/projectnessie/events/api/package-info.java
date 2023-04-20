/*
 * Copyright (C) 2023 Dremio
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

/**
 * API package for Nessie events.
 *
 * <p>The main API class is the {@link org.projectnessie.events.api.Event} interface.
 *
 * <p>Classes in this package have a Json schema that is published with this artifact.
 */
@Value.Style(
    depluralize = true,
    get = {"get*", "is*"})
@OpenAPIDefinition(
    info =
        @Info(
            title = "Nessie Events API",
            version = "${project.version}",
            contact = @Contact(name = "Project Nessie", url = "https://projectnessie.org"),
            license =
                @License(
                    name = "Apache 2.0",
                    url = "https://www.apache.org/licenses/LICENSE-2.0.html")))
package org.projectnessie.events.api;

import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.info.Contact;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.info.License;
import org.immutables.value.Value;
