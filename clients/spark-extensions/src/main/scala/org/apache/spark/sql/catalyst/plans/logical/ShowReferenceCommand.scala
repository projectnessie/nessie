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
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

case class ShowReferenceCommand(catalog: Option[String]) extends Command {

  override lazy val output: Seq[Attribute] = new StructType(
    Array[StructField](
      StructField(
        "refType",
        DataTypes.StringType,
        nullable = false,
        Metadata.empty
      ),
      StructField(
        "name",
        DataTypes.StringType,
        nullable = false,
        Metadata.empty
      ),
      StructField(
        "hash",
        DataTypes.StringType,
        nullable = false,
        Metadata.empty
      )
    )
  ).toAttributes

  override def simpleString(maxFields: Int): String = {
    s"ShowReference"
  }
}
