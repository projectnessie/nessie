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

import java.time.Clock;
import org.projectnessie.catalog.files.adls.AdlsClientSupplier;
import org.projectnessie.catalog.files.adls.AdlsObjectIO;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.gcs.GcsObjectIO;
import org.projectnessie.catalog.files.gcs.GcsStorageSupplier;
import org.projectnessie.catalog.files.s3.S3ClientSupplier;
import org.projectnessie.catalog.files.s3.S3ObjectIO;
import org.projectnessie.storage.uri.StorageUri;

public class ResolvingObjectIO extends DelegatingObjectIO {
  private final S3ObjectIO s3ObjectIO;
  private final GcsObjectIO gcsObjectIO;
  private final AdlsObjectIO adlsObjectIO;

  public ResolvingObjectIO(
      S3ClientSupplier s3ClientSupplier,
      AdlsClientSupplier adlsClientSupplier,
      GcsStorageSupplier gcsStorageSupplier) {
    this.s3ObjectIO = new S3ObjectIO(s3ClientSupplier, Clock.systemUTC());
    this.gcsObjectIO = new GcsObjectIO(gcsStorageSupplier);
    this.adlsObjectIO = new AdlsObjectIO(adlsClientSupplier);
  }

  @Override
  protected ObjectIO resolve(StorageUri uri) {
    String scheme = uri.scheme();
    if (scheme == null) {
      scheme = "file";
    }
    switch (scheme) {
      case "s3":
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
  public boolean isValidUri(StorageUri uri) {
    String scheme = uri.scheme();
    if (scheme == null) {
      return false;
    }
    switch (scheme) {
      case "s3":
      case "gs":
      case "abfs":
      case "abfss":
        return true;
      default:
        return false;
    }
  }
}
