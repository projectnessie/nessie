/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.docgen;

import com.sun.source.doctree.DocCommentTree;
import io.smallrye.config.ConfigMappingInterface.CollectionProperty;
import io.smallrye.config.ConfigMappingInterface.LeafProperty;
import io.smallrye.config.ConfigMappingInterface.MapProperty;
import io.smallrye.config.ConfigMappingInterface.MayBeOptionalProperty;
import io.smallrye.config.ConfigMappingInterface.PrimitiveProperty;
import io.smallrye.config.ConfigMappingInterface.Property;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

public class SmallRyeConfigPropertyInfo implements PropertyInfo {
  private final Property property;
  private final String propertyName;
  private final DocCommentTree doc;
  private final ExecutableElement element;

  SmallRyeConfigPropertyInfo(
      ExecutableElement element, Property property, String propertyName, DocCommentTree doc) {
    this.element = element;
    this.property = property;
    this.propertyName = propertyName;
    this.doc = doc;
  }

  @Override
  public Element propertyElement() {
    return element;
  }

  @Override
  public String propertyName() {
    return propertyName;
  }

  @Override
  public String propertySuffix() {
    if (property.isMap()) {
      ConfigPropertyName ci = element.getAnnotation(ConfigPropertyName.class);
      return ci == null || ci.value().isEmpty() ? "name" : ci.value();
    }
    return "";
  }

  @Override
  public String simplifiedTypeName() {
    return simplifiedTypeName(property);
  }

  @Override
  public Optional<Class<?>> groupType() {
    Property p = property;
    if (p.isOptional()) {
      p = p.asOptional().getNestedProperty();
    }
    if (p.isCollection()) {
      p = p.asCollection().getElement();
    }
    if (p.isMap()) {
      p = p.asMap().getValueProperty();
    }
    return p.isGroup()
        ? Optional.of(p.asGroup().getGroupType().getInterfaceType())
        : Optional.empty();
  }

  public static String simplifiedTypeName(Property property) {
    if (property.isOptional()) {
      MayBeOptionalProperty nested = property.asOptional().getNestedProperty();
      return simplifiedTypeName(nested);
    }
    if (property.isCollection()) {
      CollectionProperty coll = property.asCollection();
      Property element = coll.getElement();
      return "list of " + simplifiedTypeName(element);
    }
    if (property.isMap()) {
      MapProperty map = property.asMap();
      Property value = map.getValueProperty();
      return simplifiedTypeName(value);
    }
    if (property.isPrimitive()) {
      return property.asPrimitive().getPrimitiveType().getSimpleName();
    }
    if (property.isLeaf()) {
      LeafProperty leaf = property.asLeaf();
      Class<?> rawType = leaf.getValueRawType();
      if (rawType.isEnum()) {
        return Arrays.stream(rawType.getEnumConstants())
            .map(Enum.class::cast)
            .map(Enum::name)
            .collect(Collectors.joining(", "));
      }
      if (rawType == OptionalInt.class) {
        return "int";
      }
      if (rawType == OptionalLong.class) {
        return "long";
      }
      if (rawType == OptionalDouble.class) {
        return "double";
      }
      if (rawType == Byte.class) {
        return "byte";
      }
      if (rawType == Short.class) {
        return "short";
      }
      if (rawType == Integer.class) {
        return "int";
      }
      if (rawType == Long.class) {
        return "long";
      }
      if (rawType == Float.class) {
        return "float";
      }
      if (rawType == Double.class) {
        return "double";
      }
      if (rawType == Character.class) {
        return "char";
      }
      return rawType.getSimpleName();
    }
    if (property.isGroup()) {
      return property.asGroup().getGroupType().getInterfaceType().getSimpleName();
    }
    throw new UnsupportedOperationException("Don't know how to handle " + property);
  }

  @Override
  public DocCommentTree doc() {
    return doc;
  }

  @Override
  public String defaultValue() {
    return defaultValue(property);
  }

  public static String defaultValue(Property property) {
    if (property.isOptional()) {
      MayBeOptionalProperty nested = property.asOptional().getNestedProperty();
      return defaultValue(nested);
    }
    if (property.isPrimitive()) {
      PrimitiveProperty primitive = property.asPrimitive();
      return primitive.hasDefaultValue() ? primitive.getDefaultValue() : null;
    }
    if (property.isLeaf()) {
      LeafProperty leaf = property.asLeaf();
      return leaf.hasDefaultValue() ? leaf.getDefaultValue() : null;
    }
    return null;
  }
}
