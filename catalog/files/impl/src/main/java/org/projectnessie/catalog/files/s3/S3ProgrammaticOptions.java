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
package org.projectnessie.catalog.files.s3;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@Value.Style(allParameters = false)
@SuppressWarnings("immutables:from") // defaultOptions + buckets are not copied
public abstract class S3ProgrammaticOptions implements S3Options {

  @Override
  public abstract Optional<S3BucketOptions> defaultOptions();

  @Override
  public abstract Map<String, S3NamedBucketOptions> buckets();

  public static S3Options normalize(S3Options s3Options) {
    ImmutableS3ProgrammaticOptions.Builder builder =
        ImmutableS3ProgrammaticOptions.builder().from(s3Options);
    // not copied by from() because of different type parameters in method return types
    builder.defaultOptions(s3Options.defaultOptions());
    builder.buckets(s3Options.buckets());
    return builder.build();
  }

  @Value.Check
  protected S3ProgrammaticOptions normalizeBuckets() {
    Map<String, S3NamedBucketOptions> buckets = new HashMap<>();
    for (String bucketName : buckets().keySet()) {
      S3NamedBucketOptions options = buckets().get(bucketName);
      if (options.name().isPresent()) {
        bucketName = options.name().get();
      } else {
        options = ImmutableS3NamedBucketOptions.builder().from(options).name(bucketName).build();
      }
      if (buckets.put(bucketName, options) != null) {
        throw new IllegalArgumentException(
            "Duplicate S3 bucket name '" + bucketName + "', check your S3 bucket configurations");
      }
    }
    if (buckets.equals(buckets())) {
      return this;
    }
    return ImmutableS3ProgrammaticOptions.builder()
        .from(this)
        .defaultOptions(defaultOptions())
        .buckets(buckets)
        .build();
  }
}
