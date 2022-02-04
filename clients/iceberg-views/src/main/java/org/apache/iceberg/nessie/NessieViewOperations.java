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
package org.apache.iceberg.nessie;

import static org.apache.iceberg.view.ViewUtils.toCatalogTableIdentifier;

import java.util.Map;
import java.util.function.Predicate;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.view.BaseMetastoreViewOperations;
import org.apache.iceberg.view.ViewVersionMetadata;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NessieViewOperations extends BaseMetastoreViewOperations {
  private static final Logger LOG = LoggerFactory.getLogger(NessieViewOperations.class);
  private static final Predicate<Exception> RETRY_IF =
      exc -> !exc.getClass().getCanonicalName().contains("Unrecoverable");
  private final NessieApiV1 api;
  private final ContentKey key;
  private final UpdateableReference reference;
  private IcebergView icebergView;
  private final FileIO fileIO;
  private final Map<String, String> catalogOptions;

  NessieViewOperations(
      ContentKey key,
      UpdateableReference reference,
      NessieApiV1 api,
      FileIO fileIO,
      Map<String, String> catalogOptions) {
    this.key = key;
    this.reference = reference;
    this.api = api;
    this.fileIO = fileIO;
    this.catalogOptions = catalogOptions;
  }

  @Override
  public ViewVersionMetadata refresh() {
    try {
      reference.refresh(api);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException("Failed to refresh as ref is no longer valid.", e);
    }
    String metadataLocation = null;
    try {
      Content content =
          api.getContent().key(key).reference(reference.getReference()).get().get(key);
      LOG.debug("Content '{}' at '{}': {}", key, reference.getReference(), content);
      if (content == null) {
        if (currentMetadataLocation() != null) {
          throw new NoSuchTableException("No such view %s in %s", key, reference.getReference());
        }
      } else {
        this.icebergView =
            content
                .unwrap(IcebergView.class)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Cannot refresh iceberg view: "
                                + String.format(
                                    "Nessie points to a non-Iceberg object for path: %s.", key)));
        metadataLocation = icebergView.getMetadataLocation();
      }
    } catch (NessieNotFoundException ex) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(ex, "No such view %s", key);
      }
    }
    refreshFromMetadataLocation(metadataLocation, RETRY_IF, 2);
    return current();
  }

  private IcebergView view(TableIdentifier viewIdentifier) {
    try {
      ContentKey key = NessieUtil.toKey(viewIdentifier);
      Content view = api.getContent().key(key).reference(reference.getReference()).get().get(key);
      return view != null ? view.unwrap(IcebergView.class).orElse(null) : null;
    } catch (NessieNotFoundException e) {
      return null;
    }
  }

  @Override
  public void drop(String viewIdentifier) {
    reference.checkMutable();

    IcebergView existingView = view(toCatalogTableIdentifier(viewIdentifier));
    if (existingView == null) {
      return;
    }

    CommitMultipleOperationsBuilder commitBuilderBase =
        api.commitMultipleOperations()
            .commitMeta(
                NessieUtil.buildCommitMetadata(
                    String.format("Iceberg delete view %s", viewIdentifier), catalogOptions))
            .operation(
                Operation.Delete.of(NessieUtil.toKey(toCatalogTableIdentifier(viewIdentifier))));

    // We try to drop the view. Simple retry after ref update.
    try {
      Tasks.foreach(commitBuilderBase)
          .retry(5)
          .stopRetryOn(NessieNotFoundException.class)
          .throwFailureWhenFinished()
          .onFailure((o, exception) -> refresh())
          .run(
              commitBuilder -> {
                Branch branch = commitBuilder.branch(reference.getAsBranch()).commit();
                reference.updateReference(branch);
              },
              BaseNessieClientServerException.class);
    } catch (NessieConflictException e) {
      LOG.error("Cannot drop view: failed after retry (update ref and retry)", e);
    } catch (NessieNotFoundException e) {
      LOG.error("Cannot drop view: ref is no longer valid.", e);
    } catch (BaseNessieClientServerException e) {
      LOG.error("Cannot drop view: unknown error", e);
    }
  }

  @Override
  public void commit(
      ViewVersionMetadata base, ViewVersionMetadata metadata, Map<String, String> properties) {
    reference.checkMutable();

    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    boolean delete = true;
    try {
      ImmutableIcebergView.Builder viewBuilder = ImmutableIcebergView.builder();
      if (icebergView != null) {
        viewBuilder.id(icebergView.getId());
      }

      IcebergView newView =
          viewBuilder
              .metadataLocation(newMetadataLocation)
              .versionId(metadata.currentVersionId())
              .schemaId(metadata.definition().schema().schemaId())
              .dialect("TODO: needs to be defined in Iceberg ViewDefinition")
              .sqlText(metadata.definition().sql())
              .build();

      LOG.debug("Committing '{}' against '{}': {}", key, reference.getReference(), newView);
      ImmutableCommitMeta.Builder builder = ImmutableCommitMeta.builder();
      builder.message(buildCommitMsg(base, metadata) + " " + key.getName());
      Branch branch =
          api.commitMultipleOperations()
              .operation(Operation.Put.of(key, newView, icebergView))
              .commitMeta(NessieUtil.catalogOptions(builder, catalogOptions).build())
              .branch(reference.getAsBranch())
              .commit();
      reference.updateReference(branch);

      delete = false;
    } catch (NessieConflictException ex) {
      throw new CommitFailedException(
          ex,
          "Commit failed: Reference hash is out of date. "
              + "Update the reference %s and try again",
          reference.getName());
    } catch (HttpClientException ex) {
      // Intentionally catch all nessie-client-exceptions here and not just the "timeout" variant
      // to catch all kinds of network errors (e.g. connection reset). Network code implementation
      // details and all kinds of network devices can induce unexpected behavior. So better be
      // safe than sorry.
      delete = false;
      throw new CommitStateUnknownException(ex);
    } catch (NessieNotFoundException ex) {
      throw new RuntimeException(
          String.format("Commit failed: Reference %s no longer exist", reference.getName()), ex);
    } finally {
      if (delete) {
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  private String buildCommitMsg(ViewVersionMetadata base, ViewVersionMetadata metadata) {
    if (base != null && metadata.currentVersionId() != base.currentVersionId()) {
      return "Iceberg schema change against view ";
    }
    return "Iceberg commit against view %s";
  }

  @Override
  public FileIO io() {
    return fileIO;
  }
}
