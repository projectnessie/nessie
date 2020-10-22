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
package com.dremio.nessie.versioned.store;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;

@SuppressWarnings("checkstyle:MethodName")
public abstract class Entity {

  public static enum EntityType {
    MAP, LIST, NUMBER, STRING, BINARY, BOOLEAN, STRING_SET
  }

  private Entity() {
  }

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object other);

  public abstract EntityType getType();

  public String s() {
    throw new IllegalStateException("Not a string.");
  }

  public static Entity s(String str) {
    return new StringEntity(str);
  }

  public Set<String> ss() {
    throw new IllegalStateException("Not a string set.");
  }

  public static Entity ss(String... str) {
    return new StringSetEntity(ImmutableSet.<String>builder().add(str).build());
  }

  public static Entity ss(Set<String> strings) {
    return new StringSetEntity(strings);
  }

  public ByteString b() {
    throw new IllegalStateException("Not a binary value.");
  }

  public static Entity b(ByteString bytes) {
    return new BinaryEntity(bytes);
  }

  public static Entity b(byte[] bytes) {
    return new BinaryEntity(ByteString.copyFrom(bytes));
  }

  public List<Entity> l() {
    throw new IllegalStateException("Not a list.");
  }

  public static Entity l(List<Entity> list) {
    return new ListEntity(list);
  }

  public static Entity l(Entity... entities) {
    return new ListEntity(ImmutableList.<Entity>builder().add(entities).build());
  }


  public static Entity l(Stream<Entity> entities) {
    return new ListEntity(entities.collect(ImmutableList.toImmutableList()));
  }


  public Map<String, Entity> m() {
    throw new IllegalStateException("Not a map.");
  }

  public static Entity m(Map<String, Entity> map) {
    return new MapEntity(map);
  }

  public String n() {
    throw new IllegalStateException("Not a number.");
  }

  public static Entity n(String number) {
    return new NumberEntity(number);
  }

  public static Entity n(int number) {
    return new NumberEntity(Integer.toString(number));
  }

  public static Entity n(long number) {
    return new NumberEntity(Long.toString(number));
  }

  public boolean bl() {
    throw new IllegalStateException("Not a boolean value.");
  }

  public static Entity bl(boolean bool) {
    return new BooleanEntity(bool);
  }

  private static class StringEntity extends Entity {

    private final String str;

    public StringEntity(String s) {
      this.str = s;
    }

    public String s() {
      return str;
    }

    @Override
    public EntityType getType() {
      return EntityType.STRING;
    }

    @Override
    public int hashCode() {
      return Objects.hash(str);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof StringEntity)) {
        return false;
      }
      StringEntity other = (StringEntity) obj;
      return Objects.equals(str, other.str);
    }

  }

  private static class BinaryEntity extends Entity {

    private final ByteString binary;

    public BinaryEntity(ByteString b) {
      this.binary = b;
    }

    @Override
    public ByteString b() {
      return binary;
    }

    @Override
    public EntityType getType() {
      return EntityType.BINARY;
    }

    @Override
    public int hashCode() {
      return Objects.hash(binary);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof BinaryEntity)) {
        return false;
      }
      BinaryEntity other = (BinaryEntity) obj;
      return Objects.equals(binary, other.binary);
    }

  }

  private static class MapEntity extends Entity {

    private final ImmutableMap<String, Entity> map;

    public MapEntity(Map<String, Entity> map) {
      this.map = ImmutableMap.copyOf(map);
    }

    @Override
    public Map<String, Entity> m() {
      return map;
    }

    @Override
    public EntityType getType() {
      return EntityType.MAP;
    }

    @Override
    public int hashCode() {
      return Objects.hash(map);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof MapEntity)) {
        return false;
      }
      MapEntity other = (MapEntity) obj;
      return Objects.equals(map, other.map);
    }

  }

  private static class ListEntity extends Entity {

    private final ImmutableList<Entity> list;

    public ListEntity(List<Entity> list) {
      this.list = ImmutableList.copyOf(list);
    }

    @Override
    public List<Entity> l() {
      return list;
    }

    @Override
    public EntityType getType() {
      return EntityType.LIST;
    }

    @Override
    public int hashCode() {
      return Objects.hash(list);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ListEntity)) {
        return false;
      }
      ListEntity other = (ListEntity) obj;
      return Objects.equals(list, other.list);
    }

  }

  private static class StringSetEntity extends Entity {

    private final ImmutableSet<String> strings;

    public StringSetEntity(Set<String> strings) {
      this.strings = ImmutableSet.copyOf(strings);
    }

    @Override
    public Set<String> ss() {
      return strings;
    }

    @Override
    public EntityType getType() {
      return EntityType.STRING_SET;
    }

    @Override
    public int hashCode() {
      return Objects.hash(strings);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof StringSetEntity)) {
        return false;
      }
      StringSetEntity other = (StringSetEntity) obj;
      return Objects.equals(strings, other.strings);
    }

  }

  private static class NumberEntity extends Entity {

    private final String number;

    public NumberEntity(String number) {
      this.number = number;
    }

    public String n() {
      return number;
    }

    @Override
    public EntityType getType() {
      return EntityType.NUMBER;
    }

    @Override
    public int hashCode() {
      return Objects.hash(number);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof NumberEntity)) {
        return false;
      }
      NumberEntity other = (NumberEntity) obj;
      return Objects.equals(number, other.number);
    }

  }

  private static class BooleanEntity extends Entity {

    private final boolean bool;

    public BooleanEntity(boolean bool) {
      this.bool = bool;
    }

    @Override
    public boolean bl() {
      return bool;
    }

    @Override
    public EntityType getType() {
      return EntityType.BOOLEAN;
    }

    @Override
    public int hashCode() {
      return Objects.hash(bool);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof BooleanEntity)) {
        return false;
      }
      BooleanEntity other = (BooleanEntity) obj;
      return bool == other.bool;
    }

  }

}
