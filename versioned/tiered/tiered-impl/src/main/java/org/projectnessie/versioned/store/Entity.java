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
package org.projectnessie.versioned.store;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class Entity {

  public static enum EntityType {
    MAP,
    LIST,
    NUMBER,
    STRING,
    BINARY,
    BOOLEAN
  }

  private Entity() {}

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object other);

  public abstract EntityType getType();

  @Override
  public abstract String toString();

  public String getString() {
    throw new IllegalStateException("Not a string.");
  }

  public static Entity ofString(String str) {
    return new StringEntity(str);
  }

  public ByteString getBinary() {
    throw new IllegalStateException("Not a binary value.");
  }

  public static Entity ofBinary(ByteString bytes) {
    return new BinaryEntity(bytes);
  }

  public static Entity ofBinary(byte[] bytes) {
    return new BinaryEntity(ByteString.copyFrom(bytes));
  }

  public List<Entity> getList() {
    throw new IllegalStateException("Not a list.");
  }

  public static Entity ofList(List<Entity> list) {
    return new ListEntity(list);
  }

  public static Entity ofList(Entity... entities) {
    return new ListEntity(ImmutableList.<Entity>builder().add(entities).build());
  }

  public static Entity ofList(Stream<Entity> entities) {
    return new ListEntity(entities.collect(ImmutableList.toImmutableList()));
  }

  public Map<String, Entity> getMap() {
    throw new IllegalStateException("Not a map.");
  }

  public static Entity ofMap(Map<String, Entity> map) {
    return new MapEntity(map);
  }

  public long getNumber() {
    throw new IllegalStateException("Not a number.");
  }

  public static Entity ofNumber(int number) {
    return new NumberEntity(number);
  }

  public static Entity ofNumber(long number) {
    return new NumberEntity(number);
  }

  public boolean getBoolean() {
    throw new IllegalStateException("Not a boolean value.");
  }

  public static Entity ofBoolean(boolean bool) {
    return new BooleanEntity(bool);
  }

  private static final class StringEntity extends Entity {

    private final String str;

    public StringEntity(String s) {
      this.str = s;
    }

    @Override
    public String getString() {
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
    public String toString() {
      return "\"" + str + "\"";
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

  private static final class BinaryEntity extends Entity {

    private final ByteString binary;

    public BinaryEntity(ByteString b) {
      this.binary = b;
    }

    @Override
    public ByteString getBinary() {
      return binary;
    }

    @Override
    public EntityType getType() {
      return EntityType.BINARY;
    }

    @Override
    public String toString() {
      return "b:" + Base64.getEncoder().encodeToString(binary.toByteArray());
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

  private static final class MapEntity extends Entity {

    private final ImmutableMap<String, Entity> map;

    public MapEntity(Map<String, Entity> map) {
      this.map = ImmutableMap.copyOf(map);
    }

    @Override
    public Map<String, Entity> getMap() {
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
    public String toString() {
      return "{"
          + map.entrySet().stream()
              .map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
              .collect(Collectors.joining(", "))
          + "}";
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

  private static final class ListEntity extends Entity {

    private final ImmutableList<Entity> list;

    public ListEntity(List<Entity> list) {
      this.list = ImmutableList.copyOf(list);
    }

    @Override
    public List<Entity> getList() {
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
    public String toString() {
      return "[" + list.stream().map(Entity::toString).collect(Collectors.joining(", ")) + "]";
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

  private static final class NumberEntity extends Entity {

    private final long number;

    public NumberEntity(long number) {
      this.number = number;
    }

    public long getNumber() {
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
    public String toString() {
      return "n" + number;
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

  private static final class BooleanEntity extends Entity {

    private final boolean bool;

    public BooleanEntity(boolean bool) {
      this.bool = bool;
    }

    @Override
    public boolean getBoolean() {
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
    public String toString() {
      return Boolean.toString(bool);
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
