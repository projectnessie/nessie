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
package org.projectnessie.quarkus.config;

import io.smallrye.config.WithName;
import java.util.List;
import java.util.Optional;
import org.projectnessie.catalog.files.s3.S3BucketOptions;
import org.projectnessie.catalog.files.s3.S3ServerAuthenticationMode;

public interface CatalogS3BucketConfig extends S3BucketOptions {

  @WithName("server-auth-mode")
  @Override
  Optional<S3ServerAuthenticationMode> serverAuthenticationMode();

  @WithName("client-iam.statements")
  @Override
  Optional<List<String>> clientIamStatements();
}
