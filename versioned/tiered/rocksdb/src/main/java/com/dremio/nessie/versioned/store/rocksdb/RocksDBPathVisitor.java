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
package com.dremio.nessie.versioned.store.rocksdb;

import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.google.common.base.Preconditions;

/**
 * Visitor for creating String path representations from ExpressionPaths.
 */
class RocksDBPathVisitor implements ExpressionPath.PathVisitor<Boolean, String, RuntimeException> {
  static final RocksDBPathVisitor INSTANCE = new RocksDBPathVisitor(true);
  static final RocksDBPathVisitor INSTANCE_NO_QUOTE = new RocksDBPathVisitor(false);

  private final boolean addQuotes;

  private RocksDBPathVisitor(boolean addQuotes) {
    this.addQuotes = addQuotes;
  }

  @Override
  public String visitName(ExpressionPath.NameSegment segment, Boolean first) throws RuntimeException {
    if (addQuotes && first) {
      return "\"" + segment.getName() + visitChildOrEmpty(segment) + "\"";
    }
    return segment.getName() + visitChildOrEmpty(segment);
  }

  @Override
  public String visitPosition(ExpressionPath.PositionSegment segment, Boolean first) throws RuntimeException {
    Preconditions.checkArgument(!first);
    return segment.getPosition() + visitChildOrEmpty(segment);
  }

  private String visitChildOrEmpty(ExpressionPath.PathSegment segment) {
    return segment.getChild().map(c -> "." + c.accept(this, false)).orElse("");
  }
}
