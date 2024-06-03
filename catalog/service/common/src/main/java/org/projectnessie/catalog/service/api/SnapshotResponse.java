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
package org.projectnessie.catalog.service.api;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

/**
 * Wraps a response from the catalog.
 *
 * <p>The response is either {@linkplain #entityObject() an object} or, if not present, can
 * {@linkplain #produce(OutputStream) produce the response} to an {@linkplain OutputStream output
 * stream}.
 */
public interface SnapshotResponse {
  static SnapshotResponse forEntity(
      Reference effectiveReference,
      Object result,
      String fileName,
      String contentType,
      ContentKey contentKey,
      Content content,
      NessieEntitySnapshot<?> nessieSnapshot) {
    return new SnapshotResponse() {
      @Override
      public Optional<Object> entityObject() {
        return Optional.of(result);
      }

      @Override
      public Reference effectiveReference() {
        return effectiveReference;
      }

      @Override
      public String fileName() {
        return fileName;
      }

      @Override
      public String contentType() {
        return contentType;
      }

      @Override
      public ContentKey contentKey() {
        return contentKey;
      }

      @Override
      public Content content() {
        return content;
      }

      @Override
      public NessieEntitySnapshot<?> nessieSnapshot() {
        return nessieSnapshot;
      }

      @Override
      public void produce(OutputStream outputStream) throws IOException {
        throw new UnsupportedOperationException();
      }
    };
  }

  Reference effectiveReference();

  Optional<Object> entityObject();

  String fileName();

  String contentType();

  ContentKey contentKey();

  Content content();

  NessieEntitySnapshot<?> nessieSnapshot();

  void produce(OutputStream outputStream) throws IOException;
}
