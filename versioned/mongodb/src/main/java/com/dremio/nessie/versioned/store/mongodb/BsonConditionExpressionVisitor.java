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
package com.dremio.nessie.versioned.store.mongodb;

import java.util.stream.Collectors;

import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ConditionExpressionVisitor;

/**
 * This class allows retrieval of ConditionExpression objects in BSON format.
 */
public class BsonConditionExpressionVisitor implements ConditionExpressionVisitor<Bson> {
  private static final BsonValueVisitor EXPR_VISITOR = new BsonValueVisitor();

  /**
  * This is a callback method that ConditionExpression will call when this visitor is accepted.
  * It creates a BSON representation of the ConditionExpression object.
  * @param conditionExpression to be converted into BSON.
  * @return the converted ConditionExpression object in BSON format.
  */
  @Override
  public Bson visit(final ConditionExpression conditionExpression) {
    final String functions = conditionExpression.getFunctions().stream()
        .map(f -> f.accept(EXPR_VISITOR))
        .collect(Collectors.joining(", "));

    // This intentionally builds up a JSON representation of the query, and then parses it back to a BSON Object
    // representation. The reason for this is that the Value hierarchy in Nessie is a combination of Bson and BsonValue
    // in MongoDB parlance, and those objects don't share a common parent, which makes the standard visitor need to
    // return both.
    return BsonDocument.parse(String.format("{\"$and\": [%s]}", functions));
  }
}
