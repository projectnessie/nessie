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
    Path fileExtremelyNested = dir.resolve("smallrye-extremely_nested.md");
    Path fileVeryNested = dir.resolve("smallrye-very_nested.md");

    soft.assertThat(fileMyPrefix)
        .isRegularFile()
        .content()
        .isEqualTo(
            "The docs for `my.prefix`. \n"
                + "\n"
                + " * Some    \n"
                + " * unordered    \n"
                + " * list  \n"
                + "\n"
                + "Some more text.   \n"
                + "\n"
                + " 1. one    \n"
                + " 1. two    \n"
                + " 1. three\n"
                + "\n"
                + "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `my.prefix.some-weird-name` | `some-default` | `string` | Something that configures something.  |\n"
                + "| `my.prefix.some-duration` |  | `duration` | A duration of something.  |\n"
                + "| `my.prefix.nested.other-int` |  | `int` |  |\n"
                + "| `my.prefix.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.prefix.list-of-strings` |  | `list of string` | Example & < > \"   € ® ©. <br><br> * ` session-iam-statements[0]= {\"Effect\":\"Allow\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/alwaysAllowed/*\"}        ` <br> * ` session-iam-statements[1]= {\"Effect\":\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blocked/*\"}        ` <br><br> |\n"
                + "| `my.prefix.some-int-thing` |  | `int` | Something int-ish.  |\n");
    soft.assertThat(fileMyTypes)
        .isRegularFile()
        .content()
        .isEqualTo(
            "Documentation for `my.types`.\n"
                + "\n"
                + "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `my.types.string` |  | `string` |  |\n"
                + "| `my.types.optional-string` |  | `string` |  |\n"
                + "| `my.types.duration` |  | `duration` |  |\n"
                + "| `my.types.optional-duration` |  | `duration` |  |\n"
                + "| `my.types.path` |  | `path` |  |\n"
                + "| `my.types.optional-path` |  | `path` |  |\n"
                + "| `my.types.uri` |  | `uri` |  |\n"
                + "| `my.types.optional-uri` |  | `uri` |  |\n"
                + "| `my.types.instant` |  | `instant` |  |\n"
                + "| `my.types.optional-instant` |  | `instant` |  |\n"
                + "| `my.types.string-list` |  | `list of string` |  |\n"
                + "| `my.types.string-string-map.`_`<stringkey>`_ |  | `string` |  |\n"
                + "| `my.types.string-duration-map.`_`<key2>`_ |  | `duration` |  |\n"
                + "| `my.types.string-path-map.`_`<name>`_ |  | `path` |  |\n"
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
                + "| `my.types.bool-boxed` |  | `boolean` |  |\n"
                + "| `my.types.bool-prim` |  | `boolean` |  |\n"
                + "| `my.types.enum-thing` |  | `ONE, TWO, THREE` |  |\n"
                + "| `my.types.optional-enum` |  | `ONE, TWO, THREE` |  |\n"
                + "| `my.types.list-of-enum` |  | `list of ONE, TWO, THREE` |  |\n"
                + "| `my.types.map-to-enum.`_`<name>`_ |  | `ONE, TWO, THREE` |  |\n"
                + "| `my.types.optional-bool` |  | `boolean` |  |\n"
                + "| `my.types.mapped-a.some-weird-name` | `some-default` | `string` | Something that configures something.  |\n"
                + "| `my.types.mapped-a.some-duration` |  | `duration` | A duration of something.  |\n"
                + "| `my.types.mapped-a.nested.other-int` |  | `int` |  |\n"
                + "| `my.types.mapped-a.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.types.mapped-a.list-of-strings` |  | `list of string` | Example & < > \"   € ® ©. <br><br> * ` session-iam-statements[0]= {\"Effect\":\"Allow\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/alwaysAllowed/*\"}        ` <br> * ` session-iam-statements[1]= {\"Effect\":\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blocked/*\"}        ` <br><br> |\n"
                + "| `my.types.mapped-a.some-int-thing` |  | `int` | Something int-ish.  |\n"
                + "| `my.types.optional-mapped-a.some-weird-name` | `some-default` | `string` | Something that configures something.  |\n"
                + "| `my.types.optional-mapped-a.some-duration` |  | `duration` | A duration of something.  |\n"
                + "| `my.types.optional-mapped-a.nested.other-int` |  | `int` |  |\n"
                + "| `my.types.optional-mapped-a.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.types.optional-mapped-a.list-of-strings` |  | `list of string` | Example & < > \"   € ® ©. <br><br> * ` session-iam-statements[0]= {\"Effect\":\"Allow\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/alwaysAllowed/*\"}        ` <br> * ` session-iam-statements[1]= {\"Effect\":\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blocked/*\"}        ` <br><br> |\n"
                + "| `my.types.optional-mapped-a.some-int-thing` |  | `int` | Something int-ish.  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.some-weird-name` | `some-default` | `string` | Something that configures something.  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.some-duration` |  | `duration` | A duration of something.  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.nested.other-int` |  | `int` |  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.list-of-strings` |  | `list of string` | Example & < > \"   € ® ©. <br><br> * ` session-iam-statements[0]= {\"Effect\":\"Allow\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/alwaysAllowed/*\"}        ` <br> * ` session-iam-statements[1]= {\"Effect\":\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blocked/*\"}        ` <br><br> |\n"
                + "| `my.types.map-string-mapped-a.`_`<mappy>`_`.some-int-thing` |  | `int` | Something int-ish.  |\n"
                + "| `my.types.some-duration` |  | `duration` | A duration of something.  |\n"
                + "| `my.types.config-option-foo` |  | `string` | Something that configures something.  |\n"
                + "| `my.types.some-int-thing` |  | `int` | Something int-ish.  |\n");
    soft.assertThat(fileExtremelyNested)
        .isRegularFile()
        .content()
        .isEqualTo(
            "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `extremely.nested.extremely-nested` |  | `int` | Extremely nested.  |\n"
                + "| `extremely.nested.nested-a1` |  | `int` | A1.  |\n"
                + "| `extremely.nested.nested-a2` |  | `int` | A2.  |\n"
                + "| `extremely.nested.nested-b1` |  | `int` | B1.  |\n"
                + "| `extremely.nested.nested-a11` |  | `int` | A11.  |\n"
                + "| `extremely.nested.nested-b12` |  | `int` | B12.  |\n");
    soft.assertThat(fileVeryNested)
        .isRegularFile()
        .content()
        .isEqualTo(
            "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `very.nested.very-nested` |  | `int` | Very nested.  |\n"
                + "| `very.nested.nested-a1` |  | `int` | A1.  |\n"
                + "| `very.nested.nested-a2` |  | `int` | A2.  |\n"
                + "| `very.nested.nested-b1` |  | `int` | B1.  |\n"
                + "| `very.nested.nested-a11` |  | `int` | A11.  |\n"
                + "| `very.nested.nested-b12` |  | `int` | B12.  |\n");

    Path fileSectionA = dir.resolve("smallrye-my_types_Section_A.md");
    soft.assertThat(fileSectionA)
        .isRegularFile()
        .content()
        .isEqualTo(
            "Another map of string to `MappedA`, in its own section.\n"
                + "\n"
                + "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `my.types.map.py.`_`<name>`_`.some-weird-name` | `some-default` | `string` | Something that configures something.  |\n"
                + "| `my.types.map.py.`_`<name>`_`.some-duration` |  | `duration` | A duration of something.  |\n"
                + "| `my.types.map.py.`_`<name>`_`.nested.other-int` |  | `int` |  |\n"
                + "| `my.types.map.py.`_`<name>`_`.nested.boxed-double` |  | `double` | <br><br>_Deprecated_  |\n"
                + "| `my.types.map.py.`_`<name>`_`.list-of-strings` |  | `list of string` | Example & < > \"   € ® ©. <br><br> * ` session-iam-statements[0]= {\"Effect\":\"Allow\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/alwaysAllowed/*\"}        ` <br> * ` session-iam-statements[1]= {\"Effect\":\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blocked/*\"}        ` <br><br> |\n"
                + "| `my.types.map.py.`_`<name>`_`.some-int-thing` |  | `int` | Something int-ish.  |\n");

    // Nested sections
    Path fileNestedRoot = dir.resolve("smallrye-nested_root.md");
    soft.assertThat(fileNestedRoot)
        .isRegularFile()
        .content()
        .isEqualTo(
            "Doc for NestedSectionsRoot.\n"
                + "\n"
                + "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `nested.root.nested-c.int-not-in-c` |  | `int` |  |\n");

    Path fileNestedA = dir.resolve("smallrye-nested_root_section_a.md");
    soft.assertThat(fileNestedA)
        .isRegularFile()
        .content()
        .isEqualTo(
            "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `nested.root.nested-a.section-a.string-in-a` |  | `string` |  |\n");

    Path fileNestedB = dir.resolve("smallrye-nested_root_section_b.md");
    soft.assertThat(fileNestedB)
        .isRegularFile()
        .content()
        .isEqualTo(
            "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `nested.root.nested-b.section-b.string-in-b` |  | `string` |  |\n");

    Path fileNestedC = dir.resolve("smallrye-nested_root_section_c.md");
    soft.assertThat(fileNestedC)
        .isRegularFile()
        .content()
        .isEqualTo(
            "| Property | Default Value | Type | Description |\n"
                + "|----------|---------------|------|-------------|\n"
                + "| `nested.root.nested-c.string-in-c` |  | `string` |  |\n"
                + "| `nested.root.nested-c.int-in-c` |  | `int` |  |\n");
  }
}
