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

    Path fileProps = dir.resolve("props-.md");
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
                + "| `my.prefix.someDuration` |  | `Duration` | A duration of something.  |\n"
                + "| `my.prefix.nested` |  | `OtherMapped` |  |\n"
                + "| `my.prefix.nested.otherInt` |  | `int` |  |\n"
                + "| `my.prefix.nested.boxedDouble` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.prefix.someIntThing` |  | `int` | Something int-ish.  |\n");
    soft.assertThat(fileMyTypes)
        .isRegularFile()
        .content()
        .isEqualTo(
            "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `my.types.string` |  | `String` |  |\n"
                + "| `my.types.optionalString` |  | `String` |  |\n"
                + "| `my.types.duration` |  | `Duration` |  |\n"
                + "| `my.types.optionalDuration` |  | `Duration` |  |\n"
                + "| `my.types.path` |  | `Path` |  |\n"
                + "| `my.types.optionalPath` |  | `Path` |  |\n"
                + "| `my.types.stringList` |  | `list of String` |  |\n"
                + "| `my.types.stringStringMap` |  | `String` |  |\n"
                + "| `my.types.stringDurationMap` |  | `Duration` |  |\n"
                + "| `my.types.stringPathMap` |  | `Path` |  |\n"
                + "| `my.types.optionalInt` |  | `int` |  |\n"
                + "| `my.types.optionalLong` |  | `long` |  |\n"
                + "| `my.types.optionalDouble` |  | `double` |  |\n"
                + "| `my.types.intBoxed` |  | `int` |  |\n"
                + "| `my.types.longBoxed` |  | `long` |  |\n"
                + "| `my.types.doubleBoxed` |  | `double` |  |\n"
                + "| `my.types.floatBoxed` |  | `float` |  |\n"
                + "| `my.types.intPrim` |  | `int` |  |\n"
                + "| `my.types.longPrim` |  | `long` |  |\n"
                + "| `my.types.doublePrim` |  | `double` |  |\n"
                + "| `my.types.floatPrim` |  | `float` |  |\n"
                + "| `my.types.boolBoxed` |  | `Boolean` |  |\n"
                + "| `my.types.boolPrim` |  | `boolean` |  |\n"
                + "| `my.types.enumThing` |  | `ONE, TWO, THREE` |  |\n"
                + "| `my.types.optionalEnum` |  | `ONE, TWO, THREE` |  |\n"
                + "| `my.types.listOfEnum` |  | `list of ONE, TWO, THREE` |  |\n"
                + "| `my.types.mapToEnum` |  | `ONE, TWO, THREE` |  |\n"
                + "| `my.types.optionalBool` |  | `Boolean` |  |\n"
                + "| `my.types.mappedA` |  | `MappedA` | My `MappedA`.  |\n"
                + "| `my.types.mappedA.some-weird-name` | `some-default` | `String` | Something that configures something.  |\n"
                + "| `my.types.mappedA.someDuration` |  | `Duration` | A duration of something.  |\n"
                + "| `my.types.mappedA.nested` |  | `OtherMapped` |  |\n"
                + "| `my.types.mappedA.nested.otherInt` |  | `int` |  |\n"
                + "| `my.types.mappedA.nested.boxedDouble` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.types.mappedA.someIntThing` |  | `int` | Something int-ish.  |\n"
                + "| `my.types.optionalMappedA` |  | `MappedA` | Optional `MappedA`.  |\n"
                + "| `my.types.optionalMappedA.some-weird-name` | `some-default` | `String` | Something that configures something.  |\n"
                + "| `my.types.optionalMappedA.someDuration` |  | `Duration` | A duration of something.  |\n"
                + "| `my.types.optionalMappedA.nested` |  | `OtherMapped` |  |\n"
                + "| `my.types.optionalMappedA.nested.otherInt` |  | `int` |  |\n"
                + "| `my.types.optionalMappedA.nested.boxedDouble` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.types.optionalMappedA.someIntThing` |  | `int` | Something int-ish.  |\n"
                + "| `my.types.mapStringMappedA.`_`mappy`_`` |  | `MappedA` | Map of string to `MappedA`.  |\n"
                + "| `my.types.mapStringMappedA.`_`mappy`_`.some-weird-name` | `some-default` | `String` | Something that configures something.  |\n"
                + "| `my.types.mapStringMappedA.`_`mappy`_`.someDuration` |  | `Duration` | A duration of something.  |\n"
                + "| `my.types.mapStringMappedA.`_`mappy`_`.nested` |  | `OtherMapped` |  |\n"
                + "| `my.types.mapStringMappedA.`_`mappy`_`.nested.otherInt` |  | `int` |  |\n"
                + "| `my.types.mapStringMappedA.`_`mappy`_`.nested.boxedDouble` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.types.mapStringMappedA.`_`mappy`_`.someIntThing` |  | `int` | Something int-ish.  |\n"
                + "| `my.types.someDuration` |  | `Duration` | A duration of something.  |\n"
                + "| `my.types.configOptionFoo` |  | `String` | Something that configures something.  |\n"
                + "| `my.types.someIntThing` |  | `int` | Something int-ish.  |\n");
  }
}
