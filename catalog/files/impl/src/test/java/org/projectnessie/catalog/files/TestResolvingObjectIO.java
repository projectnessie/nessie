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
package org.projectnessie.catalog.files;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.util.List;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.catalog.files.adls.AdlsObjectIO;
import org.projectnessie.catalog.files.gcs.GcsObjectIO;
import org.projectnessie.catalog.files.s3.S3ObjectIO;
import org.projectnessie.storage.uri.StorageUri;

@ExtendWith({SoftAssertionsExtension.class, MockitoExtension.class})
public class TestResolvingObjectIO {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Mock protected S3ObjectIO s3ObjectIO;
  @Mock protected AdlsObjectIO adlsObjectIO;
  @Mock protected GcsObjectIO gcsObjectIO;

  protected ResolvingObjectIO resolvingObjectIO;

  @BeforeEach
  protected void setup() {
    resolvingObjectIO = new ResolvingObjectIO(s3ObjectIO, gcsObjectIO, adlsObjectIO);
  }

  @Test
  public void resolve() {
    soft.assertThat(resolvingObjectIO.resolve(StorageUri.of("s3://foo/bar"))).isSameAs(s3ObjectIO);
    soft.assertThat(resolvingObjectIO.resolve(StorageUri.of("s3a://foo/bar"))).isSameAs(s3ObjectIO);
    soft.assertThat(resolvingObjectIO.resolve(StorageUri.of("s3n://foo/bar"))).isSameAs(s3ObjectIO);
    soft.assertThat(resolvingObjectIO.resolve(StorageUri.of("gs://foo/bar"))).isSameAs(gcsObjectIO);
    soft.assertThat(resolvingObjectIO.resolve(StorageUri.of("abfs://foo/bar")))
        .isSameAs(adlsObjectIO);
    soft.assertThat(resolvingObjectIO.resolve(StorageUri.of("abfss://foo/bar")))
        .isSameAs(adlsObjectIO);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> resolvingObjectIO.resolve(StorageUri.of("http://foo/bar")));
  }

  @Test
  public void read() throws Exception {
    StorageUri uri = StorageUri.of("s3://foo/bar");
    when(s3ObjectIO.readObject(uri))
        .thenReturn(new ByteArrayInputStream("hello s3".getBytes(UTF_8)));

    soft.assertThat(resolvingObjectIO.readObject(uri)).hasContent("hello s3");

    verify(s3ObjectIO).readObject(uri);
    verifyNoMoreInteractions(s3ObjectIO);
    verifyNoInteractions(gcsObjectIO);
    verifyNoInteractions(adlsObjectIO);
  }

  @Test
  public void deleteSingle() throws Exception {
    StorageUri uri = StorageUri.of("s3://foo/bar");

    resolvingObjectIO.deleteObjects(List.of(uri));

    verify(s3ObjectIO).deleteObjects(List.of(uri));

    verifyNoMoreInteractions(s3ObjectIO);
    verifyNoInteractions(gcsObjectIO);
    verifyNoInteractions(adlsObjectIO);
  }

  @Test
  public void deleteMultiple() throws Exception {
    StorageUri uri1 = StorageUri.of("s3://foo/bar1");
    StorageUri uri2 = StorageUri.of("s3://foo/bar2");
    StorageUri uri3 = StorageUri.of("s3://foo/bar3");

    resolvingObjectIO.deleteObjects(List.of(uri1, uri2, uri3));

    verify(s3ObjectIO).deleteObjects(List.of(uri1, uri2, uri3));

    verifyNoMoreInteractions(s3ObjectIO);
    verifyNoInteractions(gcsObjectIO);
    verifyNoInteractions(adlsObjectIO);
  }

  @Test
  public void deleteMultipleAllTypes() throws Exception {
    StorageUri uri1 = StorageUri.of("s3://foo/bar1");
    StorageUri uri2 = StorageUri.of("gs://foo/bar2");
    StorageUri uri3 = StorageUri.of("abfs://foo/bar3");
    StorageUri uri4 = StorageUri.of("s3://foo/bar4");
    StorageUri uri5 = StorageUri.of("gs://foo/bar5");
    StorageUri uri6 = StorageUri.of("abfs://foo/bar6");

    resolvingObjectIO.deleteObjects(List.of(uri1, uri2, uri3, uri4, uri5, uri6));

    verify(s3ObjectIO).deleteObjects(List.of(uri1, uri4));
    verify(gcsObjectIO).deleteObjects(List.of(uri2, uri5));
    verify(adlsObjectIO).deleteObjects(List.of(uri3, uri6));

    verifyNoMoreInteractions(s3ObjectIO);
    verifyNoMoreInteractions(gcsObjectIO);
    verifyNoMoreInteractions(adlsObjectIO);
  }
}
