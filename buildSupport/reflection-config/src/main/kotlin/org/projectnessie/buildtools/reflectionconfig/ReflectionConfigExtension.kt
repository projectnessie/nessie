/*
 * Copyright (C) 2022 Dremio
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

package org.projectnessie.buildtools.reflectionconfig

import org.gradle.api.Project

/**
 * Configuration that specifies which classes shall be mentioned in generated reflection-config.json
 * files.
 *
 * A class will end in a generated reflection-config.json file, if its superclass matches one of the
 * regex patterns in `classExtendsPatterns` or if one of its directly implemented interfaces matches
 * one of the regex patterns in `classImplementsPatterns`.
 *
 * By default the plugin scans the classes by the project's source-sets "main" + "test". It can
 * optionally consider resolvable configurations specified in `includeConfigurations`.
 *
 * Note that the plugin scans the classes using "asm" and does not consider any indirect superclass
 * nor does it consider any implicitly implementated interface.
 */
open class ReflectionConfigExtension(project: Project) {
  /**
   * A superclass must match one of these regular expressions. If this list is empty, all
   * superclasses will match.
   */
  val classExtendsPatterns = project.objects.listProperty(String::class.java)

  /**
   * Directly implemented interfaces must match one of these regular expressions. If this list is
   * empty, the class' directly implemented interfaces are not considered.
   */
  val classImplementsPatterns = project.objects.listProperty(String::class.java)

  /**
   * Resolvable configuration(s) that should be scanned for classes matching the patterns as well.
   */
  val includeConfigurations = project.objects.listProperty(String::class.java)
}
