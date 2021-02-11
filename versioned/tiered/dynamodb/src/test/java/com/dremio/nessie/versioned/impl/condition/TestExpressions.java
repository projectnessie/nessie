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
package com.dremio.nessie.versioned.impl.condition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.dynamo.AliasCollectorImpl;
import com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil;

class TestExpressions {

  private final Entity av0 = Entity.ofBoolean(true);
  private final Entity av1 = Entity.ofBoolean(false);

  private final ExpressionPath p0 = ExpressionPath.builder("p0").build();
  private final ExpressionPath p1 = ExpressionPath.builder("p1").build();
  private final ExpressionPath p2 = ExpressionPath.builder("p2").position(2).build();

  @Test
  void aliasNoop() {
    final AliasCollectorImpl c = new AliasCollectorImpl();
    final ExpressionPath aliased = ExpressionPath.builder("foo").build().alias(c);
    assertEquals("foo", aliased.asString());
    assertTrue(c.getAttributeNames().isEmpty());
    assertTrue(c.getAttributesValues().isEmpty());
  }

  @Test
  void multiAlias() {
    final AliasCollectorImpl c = new AliasCollectorImpl();
    final ExpressionPath aliased = ExpressionPath.builder("a.b").name("c.d").position(4).name("e.f").build().alias(c);
    assertEquals("#f0.#f1[4].#f2", aliased.asString());
    assertEquals("a.b", c.getAttributeNames().get("#f0"));
    assertEquals("c.d", c.getAttributeNames().get("#f1"));
    assertEquals("e.f", c.getAttributeNames().get("#f2"));
    assertTrue(c.getAttributesValues().isEmpty());
  }

  @Test
  void aliasReservedWord() {
    final AliasCollectorImpl c = new AliasCollectorImpl();
    final ExpressionPath aliased = ExpressionPath.builder("abort").build().alias(c);
    assertEquals("#abortXX", aliased.asString());
    assertEquals("abort", c.getAttributeNames().get("#abortXX"));
    assertTrue(c.getAttributesValues().isEmpty());
  }

  @Test
  void aliasSpecialCharacter() {
    final AliasCollectorImpl c = new AliasCollectorImpl();
    final ExpressionPath aliased = ExpressionPath.builder("foo.bar").build().alias(c);
    assertEquals("#f0", aliased.asString());
    assertEquals("foo.bar", c.getAttributeNames().get("#f0"));
    assertTrue(c.getAttributesValues().isEmpty());
  }

  @Test
  void aliasValue() {
    final AliasCollectorImpl c = new AliasCollectorImpl();
    final Value v0 = Value.of(av0);
    final Value v0p = v0.alias(c);
    assertEquals(":v0", v0p.getPath().asString());
    assertEquals(AttributeValueUtil.fromEntity(av0), c.getAttributesValues().get(":v0"));
    assertTrue(c.getAttributeNames().isEmpty());
  }

  @Test
  void equals() {
    final ExpressionFunction f = ExpressionFunction.equals(ExpressionPath.builder("foo").build(), av0);
    final AliasCollectorImpl c = new AliasCollectorImpl();
    final ExpressionFunction f2 = f.alias(c);
    assertEquals("foo = :v0", f2.asString());
    assertEquals(AttributeValueUtil.fromEntity(av0), c.getAttributesValues().get(":v0"));
  }

  @Test
  void listAppend() {
    final Value v0 = ExpressionFunction.appendToList(ExpressionPath.builder("p0").build(), Entity.ofList(av0));
    final AliasCollectorImpl c = new AliasCollectorImpl();
    final Value v0p = v0.alias(c);
    assertEquals("list_append(p0, :v0)", v0p.asString());
    assertEquals(AttributeValueUtil.fromEntity(av0), c.getAttributesValues().get(":v0").l().get(0));
  }

  @Test
  void updateSetClause() {
    final UpdateExpression e0 = ImmutableUpdateExpression.builder().addClauses(
        SetClause.equals(p0, av0),
        SetClause.appendToList(p1, Entity.ofList(av1))
        ).build();
    final AliasCollectorImpl c = new AliasCollectorImpl();
    final UpdateExpression e0p = e0.alias(c);
    assertEquals(" SET p0 = :v0, p1 = list_append(p1, :v1)", e0p.toUpdateExpressionString());
    assertEquals(AttributeValueUtil.fromEntity(av0), c.getAttributesValues().get(":v0"));
    assertEquals(AttributeValueUtil.fromEntity(Entity.ofList(av1)), c.getAttributesValues().get(":v1"));
  }

  @Test
  void updateRemoveClause() {
    final UpdateExpression e0 = ImmutableUpdateExpression.builder().addClauses(
        RemoveClause.of(p0),
        RemoveClause.of(p2)
        ).build();
    final AliasCollectorImpl c = new AliasCollectorImpl();
    final UpdateExpression e0p = e0.alias(c);
    assertEquals(" REMOVE p0, p2[2]", e0p.toUpdateExpressionString());
  }

  @Test
  void updateMultiClause() {
    final UpdateExpression e0 = ImmutableUpdateExpression.builder().addClauses(
        SetClause.equals(p0, av0),
        SetClause.appendToList(p1, av1),
        RemoveClause.of(p0),
        RemoveClause.of(p2)
        ).build();
    final AliasCollectorImpl c = new AliasCollectorImpl();
    final UpdateExpression e0p = e0.alias(c);
    String expected = " "
        + "SET p0 = :v0, p1 = list_append(p1, :v1) "
        + "REMOVE p0, p2[2]";
    assertEquals(expected, e0p.toUpdateExpressionString());
  }

  @Test
  void conditionExpression() {
    final AliasCollectorImpl c = new AliasCollectorImpl();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0), ExpressionFunction.equals(p1, av1)).alias(c);
    assertEquals("p0 = :v0 AND p1 = :v1", ex.toConditionExpressionString());
  }
}
