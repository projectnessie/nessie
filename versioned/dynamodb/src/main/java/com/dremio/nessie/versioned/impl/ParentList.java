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
package com.dremio.nessie.versioned.impl;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.immutables.value.Value.Immutable;

import com.google.common.collect.ImmutableList;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Describes a list of parent hashes from the current hash.
 */
@Immutable
abstract class ParentList {

  static final int MAX_PARENT_LIST_SIZE = 50;

  public static final ParentList EMPTY = ImmutableParentList.builder().addParents(Id.EMPTY).build();

  public abstract List<Id> getParents();

  public ParentList cloneWithAdditional(Id id) {
    return ImmutableParentList.builder().addAllParents(
        Stream.concat(
            Stream.of(id),
            getParents().stream())
        .limit(MAX_PARENT_LIST_SIZE)
        .collect(ImmutableList.toImmutableList()))
        .build();
  }

  public final Id getParent() {
    return getParents().get(0);
  }

  public AttributeValue toAttributeValue() {
    return AttributeValue.builder().l(getParents().stream().map(Id::toAttributeValue).collect(Collectors.toList())).build();
  }

  public static ParentList fromAttributeValue(AttributeValue value) {
    return ImmutableParentList.builder().addAllParents(value.l().stream().map(Id::fromAttributeValue).collect(Collectors.toList())).build();
  }

}
