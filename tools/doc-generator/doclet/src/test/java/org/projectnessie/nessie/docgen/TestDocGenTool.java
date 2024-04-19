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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(SoftAssertionsExtension.class)
public class TestDocGenTool {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void docGenTool(@TempDir Path dir) throws Exception {
    List<Path> classpath =
        Arrays.stream(System.getProperty("testing.libraries").split(":"))
            .map(Paths::get)
            .collect(Collectors.toList());

    DocGenTool tool = new DocGenTool(List.of(Paths.get("src/test/java")), classpath, dir, true);
    Integer result = tool.call();
    soft.assertThat(result).isEqualTo(0);

    Path fileProps = dir.resolve("props-main.md");
    Path filePropsA = dir.resolve("props-a_a_a.md");
    Path filePropsB = dir.resolve("props-b_b.md");
    soft.assertThat(fileProps)
        .isRegularFile()
        .content()
        .isEqualTo(
            "| Property | Description |\n"
                + "|----------|-------------|\n"
                + "| `property.one` | A property. \"111\" is the default value.   <br><br>Some text there. |\n"
                + "| `property.four` | Four.  |\n");
    soft.assertThat(filePropsA)
        .isRegularFile()
        .content()
        .isEqualTo(
            "| Property | Description |\n"
                + "|----------|-------------|\n"
                + "| `property.three` | Some (`value two`) more (`one two three`). <br><br> * foo    <br> * bar    <br> * baz  <br><br>blah   <br><br> * FOO    <br> * BAR    <br> * BAZ  <br><br> * foo    <br> * bar    <br> * baz  <br><br> |\n");
    soft.assertThat(filePropsB)
        .isRegularFile()
        .content()
        .isEqualTo(
            "| Property | Description |\n"
                + "|----------|-------------|\n"
                + "| `property.two` | Some summary for checkstyle. <br><br>Another property, need some words to cause a line break in the value tag here \"111\" for testing.   <br><br>Some text there.<br><br>_Deprecated_ this is deprecated because of (`property.three`). |\n"
                + "| `property.five` | Five.  |\n");

    Path fileMyPrefix = dir.resolve("smallrye-my_prefix.md");
    Path fileMyTypes = dir.resolve("smallrye-my_types.md");
    soft.assertThat(fileMyPrefix)
        .isRegularFile()
        .content()
        .isEqualTo(
            "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `my.prefix.some-weird-name` | `some-default` | `String` | Something that configures something.  |\n"
                + "| `my.prefix.some-duration` |  | `Duration` | A duration of something.  |\n"
                + "| `my.prefix.nested` |  | `OtherMapped` |  |\n"
                + "| `my.prefix.nested.other-int` |  | `int` |  |\n"
                + "| `my.prefix.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.prefix.some-int-thing` |  | `int` | Something int-ish.  |\n");
    soft.assertThat(fileMyTypes)
        .isRegularFile()
        .content()
        .isEqualTo(
            "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `my.types.string` |  | `String` |  |\n"
                + "| `my.types.optional-string` |  | `String` |  |\n"
                + "| `my.types.duration` |  | `Duration` |  |\n"
                + "| `my.types.optional-duration` |  | `Duration` |  |\n"
                + "| `my.types.path` |  | `Path` |  |\n"
                + "| `my.types.optional-path` |  | `Path` |  |\n"
                + "| `my.types.string-list` |  | `list of String` |  |\n"
                + "| `my.types.string-string-map.`_`<stringkey>`_ |  | `String` |  |\n"
                + "| `my.types.string-duration-map.`_`<key2>`_ |  | `Duration` |  |\n"
                + "| `my.types.string-path-map.`_`<name>`_ |  | `Path` |  |\n"
                + "| `my.types.optional-int` |  | `int` |  |\n"
                + "| `my.types.optional-long` |  | `long` |  |\n"
                + "| `my.types.optional-double` |  | `double` |  |\n"
                + "| `my.types.int-boxed` |  | `int` |  |\n"
                + "| `my.types.long-boxed` |  | `long` |  |\n"
                + "| `my.types.double-boxed` |  | `double` |  |\n"
                + "| `my.types.float-boxed` |  | `float` |  |\n"
                + "| `my.types.int-prim` |  | `int` |  |\n"
                + "| `my.types.long-prim` |  | `long` |  |\n"
                + "| `my.types.double-prim` |  | `double` |  |\n"
                + "| `my.types.float-prim` |  | `float` |  |\n"
                + "| `my.types.bool-boxed` |  | `Boolean` |  |\n"
                + "| `my.types.bool-prim` |  | `boolean` |  |\n"
                + "| `my.types.enum-thing` |  | `ONE, TWO, THREE` |  |\n"
                + "| `my.types.optional-enum` |  | `ONE, TWO, THREE` |  |\n"
                + "| `my.types.list-of-enum` |  | `list of ONE, TWO, THREE` |  |\n"
                + "| `my.types.map-to-enum.`_`<name>`_ |  | `ONE, TWO, THREE` |  |\n"
                + "| `my.types.optional-bool` |  | `Boolean` |  |\n"
                + "| `my.types.mapped-a` |  | `MappedA` | My `MappedA`.  |\n"
                + "| `my.types.mapped-a.some-weird-name` | `some-default` | `String` | Something that configures something.  |\n"
                + "| `my.types.mapped-a.some-duration` |  | `Duration` | A duration of something.  |\n"
                + "| `my.types.mapped-a.nested` |  | `OtherMapped` |  |\n"
                + "| `my.types.mapped-a.nested.other-int` |  | `int` |  |\n"
                + "| `my.types.mapped-a.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.types.mapped-a.some-int-thing` |  | `int` | Something int-ish.  |\n"
                + "| `my.types.optional-mapped-a` |  | `MappedA` | Optional `MappedA`.  |\n"
                + "| `my.types.optional-mapped-a.some-weird-name` | `some-default` | `String` | Something that configures something.  |\n"
                + "| `my.types.optional-mapped-a.some-duration` |  | `Duration` | A duration of something.  |\n"
                + "| `my.types.optional-mapped-a.nested` |  | `OtherMapped` |  |\n"
                + "| `my.types.optional-mapped-a.nested.other-int` |  | `int` |  |\n"
                + "| `my.types.optional-mapped-a.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.types.optional-mapped-a.some-int-thing` |  | `int` | Something int-ish.  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_ |  | `MappedA` | Map of string to `MappedA`.  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.some-weird-name` | `some-default` | `String` | Something that configures something.  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.some-duration` |  | `Duration` | A duration of something.  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.nested` |  | `OtherMapped` |  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.nested.other-int` |  | `int` |  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.some-int-thing` |  | `int` | Something int-ish.  |\n"
                + "| `my.types.some-duration` |  | `Duration` | A duration of something.  |\n"
                + "| `my.types.config-option-foo` |  | `String` | Something that configures something.  |\n"
                + "| `my.types.some-int-thing` |  | `int` | Something int-ish.  |\n");
  }
}
