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

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.iceberg.spark.procedures.BaseGcProcedure;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;
import scala.collection.Seq;

final class GcProcedureUtil {

  private GcProcedureUtil() {}

  static final String NAMESPACE = "nessie_gc";
  static final String[] NAMESPACE_ARRAY = {NAMESPACE};

  static boolean isGcNamespace(Identifier identifier) {
    return Arrays.equals(NAMESPACE_ARRAY, identifier.namespace());
  }

  static BaseGcProcedure loadGcProcedure(Identifier procedureIdentifier, TableCatalog catalog)
      throws NoSuchProcedureException {
    if (DummyProcedure.PROCEDURE_NAME.equals(procedureIdentifier.name())) {
      return new DummyProcedure(catalog);
    }
    throw new NoSuchProcedureException(procedureIdentifier);
  }

  static InternalRow internalRow(Object... columns) {
    Seq<Object> seq =
        JavaConverters.collectionAsScalaIterable(
                Arrays.stream(columns)
                    .map(
                        object ->
                            object instanceof String
                                ? UTF8String.fromString((String) object)
                                : object)
                    .collect(Collectors.toList()))
            .toSeq();
    return InternalRow.fromSeq(seq);
  }
}
