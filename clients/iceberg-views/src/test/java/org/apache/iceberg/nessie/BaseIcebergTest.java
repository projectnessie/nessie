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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtensionForDatabaseAdapter;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Tag;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;

@ExtendWith(DatabaseAdapterExtension.class)
@NessieDbAdapterName(InmemoryDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(InmemoryTestConnectionProviderSource.class)
public class BaseIcebergTest {

  @NessieDbAdapter static DatabaseAdapter databaseAdapter;

  @RegisterExtension
  static NessieJaxRsExtension server = jaxRsExtensionForDatabaseAdapter(() -> databaseAdapter);

  @TempDir public Path temp;

  protected NessieExtCatalog catalog;
  protected NessieApiV1 api;
  protected Configuration hadoopConfig;
  protected final String branch;
  protected String uri;

  public BaseIcebergTest(String branch) {
    this.branch = branch;
  }

  private void resetData() throws NessieConflictException, NessieNotFoundException {
    Branch defaultBranch = api.getDefaultBranch();
    api.getAllReferences().stream()
        .forEach(
            ref -> {
              try {
                if (ref instanceof Branch && !ref.getName().equals(defaultBranch.getName())) {
                  api.deleteBranch().branch((Branch) ref).delete();
                } else if (ref instanceof Tag) {
                  api.deleteTag().tag((Tag) ref).delete();
                }
              } catch (NessieConflictException | NessieNotFoundException e) {
                throw new RuntimeException(e);
              }
            });
    api.createReference().reference(Branch.of(branch, null)).create();
  }

  @BeforeEach
  public void beforeEach(NessieClientFactory clientFactory, @NessieClientUri URI uri)
      throws IOException {
    // The underlying NessieCatalog from Iceberg assumes v1 API
    this.uri = uri.resolve("v1").toASCIIString();
    this.api = clientFactory.make();

    resetData();

    try {
      api.createReference().reference(Branch.of(branch, null)).create();
    } catch (Exception e) {
      // ignore, already created. Can't run this in BeforeAll as quarkus hasn't disabled auth
    }

    hadoopConfig = new Configuration();
    catalog = initCatalog(branch);
  }

  NessieExtCatalog initCatalog(String ref) {
    NessieExtCatalog newCatalog = new NessieExtCatalog();
    newCatalog.setConf(hadoopConfig);
    newCatalog.initialize(
        "nessie",
        ImmutableMap.of(
            "ref",
            ref,
            CatalogProperties.URI,
            uri,
            "auth-type",
            "NONE",
            CatalogProperties.WAREHOUSE_LOCATION,
            temp.toUri().toString()));
    return newCatalog;
  }

  protected static Schema schema(int count) {
    List<Types.NestedField> fields = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      fields.add(required(i, "id" + i, Types.LongType.get()));
    }
    return new Schema(Types.StructType.of(fields).fields());
  }

  @AfterEach
  public void afterEach() throws Exception {
    try {
      if (catalog != null) {
        catalog.close();
      }
      api.close();
    } finally {
      catalog = null;
      api = null;
      hadoopConfig = null;
    }
  }

  static String writeRecordsToFile(
      Table table, Schema schema, String filename, List<Record> records) throws IOException {
    String fileLocation =
        table.location().replace("file:", "") + String.format("/data/%s.avro", filename);
    try (FileAppender<Record> writer =
        Avro.write(Files.localOutput(fileLocation)).schema(schema).named("test").build()) {
      for (Record rec : records) {
        writer.add(rec);
      }
    }
    return fileLocation;
  }
}
