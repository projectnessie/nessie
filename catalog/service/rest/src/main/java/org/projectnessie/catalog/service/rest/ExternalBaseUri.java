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
package org.projectnessie.catalog.service.rest;

import static java.lang.String.format;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.model.Validation.REF_NAME_PATH_ELEMENT_REGEX;

import java.net.URI;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

public interface ExternalBaseUri {

  URI externalBaseURI();

  default URI coreRootURI() {
    return externalBaseURI().resolve("api/");
  }

  default URI catalogBaseURI() {
    return externalBaseURI().resolve("catalog/v1/");
  }

  default URI icebergBaseURI() {
    return externalBaseURI().resolve("iceberg/");
  }

  default boolean isNessieCatalogUri(String uri) {
    return uri.startsWith(externalBaseURI().toString());
  }

  Pattern SNAPSHOT_URI_PATTERN =
      Pattern.compile(".*/trees/" + REF_NAME_PATH_ELEMENT_REGEX + "/snapshot/(.*)$");

  default Optional<TableRef> resolveTableFromUri(String uri) {
    URI u = URI.create(uri);
    Matcher m = SNAPSHOT_URI_PATTERN.matcher(u.getPath());
    if (!m.matches()) {
      return Optional.empty();
    }

    ContentKey key = ContentKey.fromPathString(m.group(3));
    ParsedReference ref = parseRefPathString(m.group(1));

    return Optional.of(TableRef.tableRef(key, ref, null));
  }

  static ParsedReference parseRefPathString(String refPathString) {
    return resolveReferencePathElement(
        refPathString,
        Reference.ReferenceType.BRANCH,
        () -> {
          throw new IllegalArgumentException("ref path must specify a branch");
        });
  }

  default void icebergConfigDefaults(BiConsumer<String, String> config) {}

  default void icebergConfigOverrides(BiConsumer<String, String> config) {
    // Make sure that `nessie.core-base-uri` always returns a `/` terminated URI.
    config.accept("nessie.core-base-uri", coreRootURI().toString());
    // Make sure that `nessie.catalog-base-uri` always returns a `/` terminated URI.
    config.accept("nessie.catalog-base-uri", catalogBaseURI().toString());
    // Iceberg base URI exposed twice for Spark SQL extensions, which update the `uri` config
    // when the branch is changed.
    config.accept("nessie.iceberg-base-uri", icebergBaseURI().toString());
    config.accept("uri", icebergBaseURI().toString());
  }

  /**
   * The path, without a leading slash, of the URI to sign a request to S3 for a specific content
   * key.
   *
   * <p>Must use both {@code s3.signer.uri} and {@code s3.signer.endpoint}, because Iceberg before
   * 1.5.0 does not handle full URIs passed via {@code s3.signer.endpoint}. This was changed via <a
   * href="https://github.com/apache/iceberg/pull/8976/files#diff-1f7498b6989fffc169f7791292ed2ccb35b305f6a547fd832f6724057c8aca8bR213-R216">this
   * Iceberg PR</a> first released in Iceberg 1.5.0. It's unclear how other language implementations
   * deal with this.
   *
   * @param prefix the prefix of the request (warehouse and reference)
   * @param signerParam the signer params path parameter (URL safe representation)
   */
  default String icebergS3SignerPathWithPath(String prefix, String signerParam) {
    return format("v1/%s/s3sign/%s", encode(prefix, UTF_8), signerParam);
  }
}
