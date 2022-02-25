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

import static org.apache.iceberg.view.ViewUtils.validateTableIdentifier;

import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.view.BaseMetastoreViewOperations;
import org.apache.iceberg.view.BaseMetastoreViews;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;
import org.projectnessie.model.TableReference;
import org.projectnessie.model.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A catalog that supports Iceberg views. USE AT YOUR OWN RISK / EXPERIMENTAL CODE. */
public class NessieViewCatalog extends BaseMetastoreViews implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(NessieCatalog.class);
  private NessieApiV1 api;
  private String warehouseLocation;
  private Configuration config;
  private UpdateableReference reference;
  private String name;
  private FileIO fileIO;
  private Map<String, String> catalogOptions;

  public NessieViewCatalog(Configuration conf) {
    super(conf);
    this.config = conf;
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier viewIdentifier) {
    if (null != warehouseLocation && warehouseLocation.endsWith("/")) {
      warehouseLocation = warehouseLocation.substring(0, warehouseLocation.length() - 1);
    }
    return String.format(
        "%s/%s/%s", warehouseLocation, viewIdentifier.namespace(), viewIdentifier.name());
  }

  public void initialize(String inputName, Map<String, String> options) {
    this.catalogOptions = ImmutableMap.copyOf(options);
    String fileIOImpl = options.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO =
        fileIOImpl == null
            ? new HadoopFileIO(config)
            : CatalogUtil.loadFileIO(fileIOImpl, options, config);
    this.name = inputName == null ? "nessie_view_catalog" : inputName;
    // remove nessie prefix
    final Function<String, String> removePrefix =
        x -> x.replace(NessieUtil.NESSIE_CONFIG_PREFIX, "");

    this.api =
        createNessieClientBuilder(options.get(NessieUtil.CONFIG_CLIENT_BUILDER_IMPL))
            .fromConfig(x -> options.get(removePrefix.apply(x)))
            .build(NessieApiV1.class);

    this.warehouseLocation = options.get(CatalogProperties.WAREHOUSE_LOCATION);
    if (warehouseLocation == null) {
      logger.warn(
          "Catalog creation for inputName={} and options {} failed, because parameter "
              + "'warehouse' is not set, Nessie can't store data.",
          inputName,
          options);
      throw new IllegalStateException("Parameter 'warehouse' not set, Nessie can't store data.");
    }
    final String requestedRef =
        options.get(removePrefix.apply(NessieConfigConstants.CONF_NESSIE_REF));
    final String hashOnRef =
        options.get(removePrefix.apply(NessieConfigConstants.CONF_NESSIE_REF_HASH));
    this.reference = loadReference(requestedRef, hashOnRef);
  }

  private static NessieClientBuilder<?> createNessieClientBuilder(String customBuilder) {
    NessieClientBuilder<?> clientBuilder;
    if (customBuilder != null) {
      try {
        clientBuilder =
            DynMethods.builder("builder").impl(customBuilder).build().asStatic().invoke();
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to use custom NessieClientBuilder '%s'.", customBuilder), e);
      }
    } else {
      clientBuilder = HttpClientBuilder.builder();
    }
    return clientBuilder;
  }

  @Override
  public void close() {
    api.close();
  }

  public void refresh() throws NessieNotFoundException {
    this.reference.refresh(this.api);
  }

  private UpdateableReference loadReference(String requestedRef, String hash) {
    try {
      Reference ref =
          requestedRef == null
              ? api.getDefaultBranch()
              : api.getReference().refName(requestedRef).get();
      if (hash != null) {
        if (ref instanceof Branch) {
          ref = Branch.of(ref.getName(), hash);
        } else {
          ref = Tag.of(ref.getName(), hash);
        }
      }
      return new UpdateableReference(ref, hash != null);
    } catch (NessieNotFoundException ex) {
      if (requestedRef != null) {
        throw new IllegalArgumentException(
            String.format(
                "Nessie ref '%s' does not exist. This ref must exist before creating a NessieViewCatalog.",
                requestedRef),
            ex);
      }

      throw new IllegalArgumentException(
          String.format(
              "Nessie does not have an existing default branch."
                  + "Either configure an alternative ref via %s or create the default branch on the server.",
              NessieConfigConstants.CONF_NESSIE_REF),
          ex);
    }
  }

  protected BaseMetastoreViewOperations newViewOps(TableIdentifier viewIdentifier) {
    validateTableIdentifier(viewIdentifier);

    TableReference tr = TableReference.parse(viewIdentifier.name());
    Preconditions.checkArgument(
        !tr.hasTimestamp(),
        "Invalid view name: # is only allowed for hashes (reference by "
            + "timestamp is not supported)");
    UpdateableReference newReference = this.reference;
    if (tr.getReference() != null) {
      newReference = loadReference(tr.getReference(), tr.getHash());
    }

    return new NessieViewOperations(
        ContentKey.of(
            org.projectnessie.model.Namespace.of(viewIdentifier.namespace().levels()),
            tr.getName()),
        newReference,
        api,
        fileIO,
        catalogOptions);
  }
}
