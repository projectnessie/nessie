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
package org.projectnessie.catalog.files.gcs;

import java.util.List;
import java.util.Map;
import org.projectnessie.cel.EnvOption;
import org.projectnessie.cel.Library;
import org.projectnessie.cel.ProgramOption;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.Err;
import org.projectnessie.cel.common.types.StringT;
import org.projectnessie.cel.interpreter.functions.FunctionOp;
import org.projectnessie.cel.interpreter.functions.Overload;
import org.projectnessie.cel.relocated.com.google.api.expr.v1alpha1.Decl;
import org.projectnessie.cel.relocated.com.google.api.expr.v1alpha1.Type;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptCreateException;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.types.jackson.JacksonRegistry;

/** CEL Utilities used by {@link TestGcsDownscopedCredentials}. */
final class CelEval {

  /** Simulates the {@code api.getAttribute()} function in GCP IAM expressions. */
  static final FunctionOp apiGetAttribute =
      values -> {
        try {
          ApiInterface apiInterface = (ApiInterface) values[0].value();
          String key = (String) values[1].value();
          String def = (String) values[2].value();
          return StringT.stringOf(apiInterface.attributes.getOrDefault(key, def));
        } catch (RuntimeException e) {
          return Err.newErr(e, "%s", e.getMessage());
        }
      };

  /** Type for {@code api} provided to GCP IAM expressions. */
  static final Type apiType = Decls.newObjectType(ApiInterface.class.getName());

  /** Type the {@code resource} object provided to GCP IAM expressions. */
  static final Type resourceType = Decls.newObjectType(ResourceComposite.class.getName());

  /** CEL library for the {@code api} object. */
  static final Library apiLib =
      new Library() {
        @Override
        public List<EnvOption> getCompileOptions() {
          return List.of(
              EnvOption.declarations(
                  Decls.newFunction(
                      "getAttribute",
                      Decls.newInstanceOverload(
                          "api_get_attribute_str_str_str",
                          List.of(apiType, Decls.String, Decls.String),
                          Decls.String))
                  //
                  ));
        }

        @Override
        public List<ProgramOption> getProgramOptions() {
          return List.of(
              ProgramOption.functions(Overload.function("getAttribute", apiGetAttribute))
              //
              );
        }
      };

  static final List<Decl> decls =
      List.of(
          Decls.newVar("resource", resourceType), Decls.newVar("api", apiType)
          //
          );
  static final List<Object> types = List.of(ResourceComposite.class);

  private static final ScriptHost SCRIPT_HOST =
      ScriptHost.newBuilder().registry(JacksonRegistry.newRegistry()).build();

  public static Script scriptFor(String expr) throws ScriptCreateException {
    return SCRIPT_HOST
        .buildScript(expr)
        .withDeclarations(decls)
        .withTypes(types)
        .withLibraries(apiLib)
        .build();
  }

  /** Necessary type for the Google CEL IAM {@code api} object. */
  static final class ApiInterface {
    final Map<String, String> attributes;

    ApiInterface(Map<String, String> attributes) {
      this.attributes = attributes;
    }
  }

  /** Necessary type for the Google CEL IAM {@code resource} object. */
  static class ResourceComposite {
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }
}
