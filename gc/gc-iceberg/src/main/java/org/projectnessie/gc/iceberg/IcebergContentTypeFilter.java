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
package org.projectnessie.gc.iceberg;

import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.projectnessie.gc.identify.ContentTypeFilter;
import org.projectnessie.model.Content;

public final class IcebergContentTypeFilter implements ContentTypeFilter {

  public static final IcebergContentTypeFilter INSTANCE = new IcebergContentTypeFilter();

  private IcebergContentTypeFilter() {}

  private static final Set<Content.Type> ICEBERG = ImmutableSet.of(ICEBERG_TABLE, ICEBERG_VIEW);

  @Override
  public boolean test(Content.Type type) {
    return type == ICEBERG_TABLE || type == ICEBERG_VIEW;
  }

  @Override
  public Set<Content.Type> validTypes() {
    return ICEBERG;
  }
}
