/**
 * Copyright (C) 2020 Dremio
 *
 *             Licensed under the Apache License, Version 2.0 (the "License");
 *             you may not use this file except in compliance with the License.
 *             You may obtain a copy of the License at
 *
 *             http://www.apache.org/licenses/LICENSE-2.0
 *
 *             Unless required by applicable law or agreed to in writing, software
 *             distributed under the License is distributed on an "AS IS" BASIS,
 *             WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *             See the License for the specific language governing permissions and
 *             limitations under the License.
 */
package com.dremio.iceberg.deltalake

import java.io.IOException

import com.google.common.collect.Lists
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.delta.storage.LogStore

import scala.collection.JavaConverters

abstract class LogStoreWrapper extends LogStore {

  @throws[IOException]
  def readImpl(path: Path): java.util.List[String]

  @throws[IOException]
  def listFromImpl(path: Path): java.util.List[FileStatus]

  @throws[IOException]
  def writeImpl(path: Path, actions: java.util.List[String], overwrite: Boolean): Unit

  def read(path: Path): Seq[String] = {
    JavaConverters.asScalaIteratorConverter(readImpl(path).iterator).asScala.toSeq
  }

  def listFrom(path: Path): Iterator[FileStatus] = {
    JavaConverters.asScalaIteratorConverter(listFromImpl(path).iterator).asScala
  }


  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    writeImpl(path, Lists.newArrayList(JavaConverters.asJavaIteratorConverter(actions).asJava), overwrite)
  }
}
