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
package org.projectnessie.catalog.files.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableS3NamedBucketOptions.class)
@JsonDeserialize(as = ImmutableS3NamedBucketOptions.class)
@SuppressWarnings("immutables:subtype")
public interface S3NamedBucketOptions extends S3BucketOptions {

  S3BucketOptions FALLBACK = ImmutableS3NamedBucketOptions.builder().build();

  /**
   * The name of the bucket. If unset, the name of the bucket will be extracted from the
   * configuration option, e.g. if {@code nessie.catalog.service.s3.bucket1.name=my-bucket} is set,
   * the bucket name will be {@code my-bucket}; otherwise, it will be {@code bucket1}.
   *
   * <p>This should only be defined if the bucket name contains non-alphanumeric characters, such as
   * dots or dashes.
   */
  Optional<String> name();

  @Value.NonAttribute
  @JsonIgnore
  default S3NamedBucketOptions deepCopy() {
    return ImmutableS3NamedBucketOptions.builder()
        .from(S3BucketOptions.super.deepCopy())
        .name(name())
        .build();
  }
}
