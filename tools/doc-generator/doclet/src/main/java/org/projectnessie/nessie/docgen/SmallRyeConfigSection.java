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
import java.util.List;
import javax.lang.model.element.TypeElement;

public class SmallRyeConfigSection {

  private final String fileName;
  private final String prefix;
  private final List<SmallRyeConfigPropertyInfo> properties;
  private final TypeElement element;
  private final DocCommentTree typeComment;

  SmallRyeConfigSection(
      String fileName,
      String prefix,
      List<SmallRyeConfigPropertyInfo> properties,
      TypeElement element,
      DocCommentTree typeComment) {
    this.fileName = fileName;
    this.prefix = prefix;
    this.properties = properties;
    this.element = element;
    this.typeComment = typeComment;
  }

  public String fileName() {
    return fileName;
  }

  public String prefix() {
    return prefix;
  }

  public List<SmallRyeConfigPropertyInfo> properties() {
    return properties;
  }

  public TypeElement element() {
    return element;
  }

  public DocCommentTree typeComment() {
    return typeComment;
  }
}
