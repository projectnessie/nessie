/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.gc.iceberg;

import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.iceberg.catalog.Procedure;

/**
 * Extends {@link SparkCatalog} to load the Nessie GC procedures in "nessie_gc" namespace. As there
 * is no other way to "plug in" custom procedures in Iceberg yet.
 *
 * <p>This extension can be removed after Iceberg supports pluggable stored procedures.
 */
@SuppressWarnings("unused")
public class NessieIcebergGcSparkCatalog extends SparkCatalog {

  public NessieIcebergGcSparkCatalog() {
    super();
  }

  @Override
  public Procedure loadProcedure(Identifier procedureIdentifier) throws NoSuchProcedureException {
    if (GcProcedureUtil.isGcNamespace(procedureIdentifier)) {
      return GcProcedureUtil.loadGcProcedure(procedureIdentifier, this);
    }
    return super.loadProcedure(procedureIdentifier);
  }
}
