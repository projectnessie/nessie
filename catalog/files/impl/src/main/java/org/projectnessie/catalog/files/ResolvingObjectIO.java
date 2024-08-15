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

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.projectnessie.catalog.files.adls.AdlsClientSupplier;
import org.projectnessie.catalog.files.adls.AdlsObjectIO;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.gcs.GcsObjectIO;
import org.projectnessie.catalog.files.gcs.GcsStorageSupplier;
import org.projectnessie.catalog.files.s3.S3ClientSupplier;
import org.projectnessie.catalog.files.s3.S3CredentialsResolver;
import org.projectnessie.catalog.files.s3.S3ObjectIO;
import org.projectnessie.storage.uri.StorageUri;

public class ResolvingObjectIO extends DelegatingObjectIO {
  private final S3ObjectIO s3ObjectIO;
  private final GcsObjectIO gcsObjectIO;
  private final AdlsObjectIO adlsObjectIO;

  public ResolvingObjectIO(
      S3ClientSupplier s3ClientSupplier,
      S3CredentialsResolver s3CredentialsResolver,
      AdlsClientSupplier adlsClientSupplier,
      GcsStorageSupplier gcsStorageSupplier) {
    this(
        new S3ObjectIO(s3ClientSupplier, s3CredentialsResolver),
        new GcsObjectIO(gcsStorageSupplier),
        new AdlsObjectIO(adlsClientSupplier));
  }

  public ResolvingObjectIO(
      S3ObjectIO s3ObjectIO, GcsObjectIO gcsObjectIO, AdlsObjectIO adlsObjectIO) {
    this.s3ObjectIO = s3ObjectIO;
    this.gcsObjectIO = gcsObjectIO;
    this.adlsObjectIO = adlsObjectIO;
  }

  @Override
  protected ObjectIO resolve(StorageUri uri) {
    String scheme = uri.scheme();
    if (scheme == null) {
      scheme = "file";
    }
    switch (scheme) {
      case "s3":
      case "s3a":
      case "s3n":
        return s3ObjectIO;
      case "gs":
        return gcsObjectIO;
      case "abfs":
      case "abfss":
        return adlsObjectIO;
      default:
        throw new IllegalArgumentException("Unknown or unsupported scheme: " + scheme);
    }
  }

  @Override
  public void deleteObjects(List<StorageUri> uris) throws IOException {
    IOException ex = null;
    Map<ObjectIO, List<StorageUri>> perObjectIO = new IdentityHashMap<>();
    uris.forEach(uri -> perObjectIO.computeIfAbsent(resolve(uri), x -> new ArrayList<>()).add(uri));
    for (Map.Entry<ObjectIO, List<StorageUri>> perObjIO : perObjectIO.entrySet()) {
      try {
        perObjIO.getKey().deleteObjects(perObjIO.getValue());
      } catch (IOException e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }
}
