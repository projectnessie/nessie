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
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

case class CreateReferenceField(
    reference: String,
    isBranch: Boolean,
    catalog: Option[String],
    fromReference: Option[String]
) extends Command {

  override lazy val output: Seq[Attribute] = new StructType(
    Array[StructField](
      StructField("refType", DataTypes.StringType, false, Metadata.empty),
      StructField("name", DataTypes.StringType, false, Metadata.empty),
      StructField("hash", DataTypes.StringType, false, Metadata.empty)
    )
  ).toAttributes

  override def simpleString(maxFields: Int): String = {
    s"CreateReference ${reference}"
  }
}
