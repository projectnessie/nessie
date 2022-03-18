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
package org.projectnessie.deltalake;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.projectnessie.model.ContentKey;

/**
 * Utility class that produces a {@link ContentKey} instance from a Hadoop {@link Path} or a file
 * path string.
 */
public class DeltaContentKeyUtil {

  private DeltaContentKeyUtil() {}

  static final String INVALID_PATH_MSG = "Cannot produce ContentKey from invalid path '%s'";
  static final String INVALID_HADOOP_PATH_MSG =
      "Cannot produce ContentKey from invalid hadoop path '%s'";

  public static ContentKey fromHadoopPath(Path path) {
    Preconditions.checkArgument(null != path, INVALID_HADOOP_PATH_MSG, path);
    return fromFilePathString(path.toUri().getPath());
  }

  public static ContentKey fromFilePathString(String path) {
    Preconditions.checkArgument(null != path && !path.isEmpty(), INVALID_PATH_MSG, path);
    // we can't simply do a path.split("/") as that would result with
    // "/tmp/a/b/c in a string array where the first element is an empty string,
    // which isn't supported by ContentKey
    String[] elements = StringUtils.stripEnd(StringUtils.stripStart(path, "/"), "/").split("/");
    Preconditions.checkState(
        elements.length > 0 && !Arrays.asList(elements).contains(""), INVALID_PATH_MSG, path);
    return ContentKey.of(elements);
  }
}
