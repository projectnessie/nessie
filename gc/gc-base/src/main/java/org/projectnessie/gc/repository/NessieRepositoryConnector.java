/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Reference;

public final class NessieRepositoryConnector implements RepositoryConnector {

  private final NessieApiV1 api;

  private NessieRepositoryConnector(NessieApiV1 api) {
    this.api = api;
  }

  public static RepositoryConnector nessie(NessieApiV1 api) {
    return new NessieRepositoryConnector(api);
  }

  @Override
  public Stream<Reference> allReferences() throws NessieNotFoundException {
    return api.getAllReferences().stream();
  }

  @Override
  public Stream<LogResponse.LogEntry> commitLog(Reference ref) throws NessieNotFoundException {
    return api.getCommitLog().reference(ref).fetch(FetchOption.ALL).stream();
  }

  @Override
  public Stream<Entry<ContentKey, Content>> allContents(Detached ref, Set<Content.Type> types) {
    return StreamSupport.stream(new BatchContentSplit(ref, types), false);
  }

  private class BatchContentSplit extends AbstractSpliterator<Entry<ContentKey, Content>> {
    private final Detached ref;
    private final Set<Content.Type> types;
    private Iterator<ContentKey> keysSplit = null;
    private Iterator<Entry<ContentKey, Content>> currentBatch = Collections.emptyIterator();

    BatchContentSplit(Detached ref, Set<Content.Type> types) {
      super(Long.MAX_VALUE, 0);
      this.ref = ref;
      this.types = types;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Entry<ContentKey, Content>> action) {
      if (keysSplit == null) {
        try {
          keysSplit = allContentKeys(ref, types).iterator();
        } catch (NessieNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      while (true) {
        if (currentBatch.hasNext()) {
          action.accept(currentBatch.next());
          return true;
        }

        List<ContentKey> batchKeys = new ArrayList<>();
        while (batchKeys.size() < CONTENT_BATCH_SIZE && keysSplit.hasNext()) {
          batchKeys.add(keysSplit.next());
        }

        if (batchKeys.isEmpty()) {
          return false;
        }

        try {
          currentBatch = fetchContents(ref, batchKeys).entrySet().iterator();
        } catch (NessieNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    }

    private Map<ContentKey, Content> fetchContents(Detached ref, List<ContentKey> allKeys)
        throws NessieNotFoundException {
      return api.getContent().reference(ref).keys(allKeys).get();
    }

    private Stream<ContentKey> allContentKeys(Detached ref, Set<Content.Type> types)
        throws NessieNotFoundException {
      return api.getEntries().reference(ref).stream()
          .filter(e -> types.contains(e.getType()))
          .map(EntriesResponse.Entry::getName);
    }
  }

  static final int CONTENT_BATCH_SIZE = 250;

  @Override
  public void close() {
    api.close();
  }
}
