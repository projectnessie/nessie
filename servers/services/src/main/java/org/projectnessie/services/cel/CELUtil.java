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
package org.projectnessie.services.cel;

import com.google.api.expr.v1alpha1.Decl;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.types.jackson.JacksonRegistry;
import org.projectnessie.model.CommitMeta;

/** A utility class for CEL declarations and other things. */
public class CELUtil {

  public static final String CONTAINER = "org.projectnessie.model";
  public static final ScriptHost SCRIPT_HOST =
      ScriptHost.newBuilder().registry(JacksonRegistry.newRegistry()).build();

  public static final List<Decl> COMMIT_LOG_DECLARATIONS =
      Collections.singletonList(
          Decls.newVar("commit", Decls.newObjectType(CommitMeta.class.getName())));

  public static final List<Decl> ENTRIES_DECLARATIONS =
      Arrays.asList(
          Decls.newVar("entry", Decls.newMapType(Decls.String, Decls.String)),
          Decls.newVar("namespace", Decls.String),
          Decls.newVar("contentType", Decls.String));

  public static final List<Decl> AUTHORIZATION_RULE_DECLARATIONS =
      ImmutableList.of(
          Decls.newVar("ref", Decls.String),
          Decls.newVar("path", Decls.String),
          Decls.newVar("role", Decls.String),
          Decls.newVar("op", Decls.String));

  public static final List<Object> COMMIT_LOG_TYPES = Collections.singletonList(CommitMeta.class);
}
