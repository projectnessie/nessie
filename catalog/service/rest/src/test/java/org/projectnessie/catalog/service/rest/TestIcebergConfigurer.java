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

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.catalog.service.rest.IcebergConfigurer.S3_SIGNER_ENDPOINT;
import static org.projectnessie.catalog.service.rest.IcebergConfigurer.S3_SIGNER_URI;

import java.net.URI;
import java.util.stream.Stream;
import org.assertj.core.api.MapAssert;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.model.ContentKey;

@ExtendWith(SoftAssertionsExtension.class)
public class TestIcebergConfigurer {
  @InjectSoftAssertions protected SoftAssertions soft;

  /** Verify compatibility with Iceberg < 1.5.0 S3 signer properties. */
  @ParameterizedTest
  @MethodSource
  public void icebergConfigPerTable(
      URI baseUri, String loc, String prefix, ContentKey key, String signUri, String signPath) {
    IcebergConfigurer c = new IcebergConfigurer();

    IcebergTableMetadata tm = mock(IcebergTableMetadata.class);
    when(tm.location()).thenReturn(loc);

    c.uriInfo = () -> baseUri;

    MapAssert<String, String> props =
        soft.assertThat(c.icebergConfigPerTable(tm, prefix, key, null));
    if (signUri != null) {
      props.containsEntry(S3_SIGNER_URI, signUri);
      soft.assertThat(signUri).endsWith("/");
      soft.assertThat(signPath).isNotNull();
    } else {
      props.doesNotContainKey(S3_SIGNER_URI);
    }
    if (signPath != null) {
      props.containsEntry(S3_SIGNER_ENDPOINT, signPath);
      soft.assertThat(signPath).doesNotStartWith("/");
      soft.assertThat(signUri).isNotNull();
    } else {
      props.doesNotContainKey(S3_SIGNER_ENDPOINT);
    }
  }

  static Stream<Arguments> icebergConfigPerTable() {
    ContentKey key = ContentKey.of("foo", "bar");

    String s3 = "s3://bucket/path/1/2/3";
    String gcs = "gcs://bucket/path/1/2/3";
    String complexPrefix = "main|s3://blah/meep";
    return Stream.of(
        arguments(
            URI.create("http://foo:12434"),
            s3,
            "main",
            key,
            "http://foo:12434/iceberg/",
            "v1/main/s3-sign/" + key.toPathString() + "?loc=" + encode(s3, UTF_8)),
        arguments(
            URI.create("http://foo:12434/some/long/prefix/"),
            s3,
            complexPrefix,
            key,
            "http://foo:12434/some/long/prefix/iceberg/",
            "v1/"
                + encode(complexPrefix, UTF_8)
                + "/s3-sign/"
                + key.toPathString()
                + "?loc="
                + encode(s3, UTF_8)),
        arguments(
            URI.create("https://foo/some/long/prefix/"),
            s3,
            complexPrefix,
            key,
            "https://foo/some/long/prefix/iceberg/",
            "v1/"
                + encode(complexPrefix, UTF_8)
                + "/s3-sign/"
                + key.toPathString()
                + "?loc="
                + encode(s3, UTF_8)),
        arguments(
            URI.create("http://foo:12434/some/long/prefix/"), gcs, complexPrefix, key, null, null));
  }
}
