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
package org.projectnessie.deltalake

import com.google.common.base.Preconditions
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.projectnessie.model.ContentKey

import scala.collection.JavaConverters._

object DeltaContentKeyUtil {

  val INVALID_PATH_MSG = "Cannot produce ContentKey from invalid path '%s'"
  val INVALID_HADOOP_PATH_MSG =
    "Cannot produce ContentKey from invalid hadoop path '%s'"

  def fromHadoopPath(path: Path): ContentKey = {
    Preconditions.checkArgument(null != path, INVALID_HADOOP_PATH_MSG, path)
    fromFilePathString(path.toUri.getPath)
  }

  def fromFilePathString(path: String): ContentKey = {
    Preconditions.checkArgument(
      null != path && path.nonEmpty,
      INVALID_PATH_MSG,
      path
    )
    // we can't simply do a path.split("/") as that would result with
    // "/tmp/a/b/c in a string array where the first element is an empty string,
    // which isn't supported by ContentKey
    val elements = StringUtils
      .stripEnd(StringUtils.stripStart(path, "/"), "/")
      .split("/")
      .toList
    Preconditions.checkState(
      elements.nonEmpty && !elements.contains(""),
      INVALID_PATH_MSG,
      path
    )
    ContentKey.of(elements.asJava)
  }

}
