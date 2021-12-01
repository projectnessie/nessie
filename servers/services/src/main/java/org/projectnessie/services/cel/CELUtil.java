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
import java.util.Collections;
import java.util.List;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.types.jackson.JacksonRegistry;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceMetadata;

/** A utility class for CEL declarations and other things. */
public class CELUtil {

  public static final String CONTAINER = "org.projectnessie.model";
  public static final ScriptHost SCRIPT_HOST =
      ScriptHost.newBuilder().registry(JacksonRegistry.newRegistry()).build();

  public static final String VAR_REF = "ref";
  public static final String VAR_REF_TYPE = "refType";
  public static final String VAR_REF_META = "refMeta";
  public static final String VAR_COMMIT = "commit";
  public static final String VAR_ENTRY = "entry";
  public static final String VAR_NAMESPACE = "namespace";
  public static final String VAR_CONTENT_TYPE = "contentType";
  public static final String VAR_PATH = "path";
  public static final String VAR_ROLE = "role";
  public static final String VAR_OP = "op";

  public static final List<Decl> REFERENCES_DECLARATIONS =
      ImmutableList.of(
          Decls.newVar(VAR_COMMIT, Decls.newObjectType(CommitMeta.class.getName())),
          Decls.newVar(VAR_REF, Decls.newObjectType(Reference.class.getName())),
          Decls.newVar(VAR_REF_META, Decls.newObjectType(ReferenceMetadata.class.getName())),
          Decls.newVar(VAR_REF_TYPE, Decls.String));

  public static final List<Decl> COMMIT_LOG_DECLARATIONS =
      Collections.singletonList(
          Decls.newVar(VAR_COMMIT, Decls.newObjectType(CommitMeta.class.getName())));

  public static final List<Decl> ENTRIES_DECLARATIONS =
      ImmutableList.of(
          Decls.newVar(VAR_ENTRY, Decls.newMapType(Decls.String, Decls.String)),
          Decls.newVar(VAR_NAMESPACE, Decls.String),
          Decls.newVar(VAR_CONTENT_TYPE, Decls.String));

  public static final List<Decl> AUTHORIZATION_RULE_DECLARATIONS =
      ImmutableList.of(
          Decls.newVar(VAR_REF, Decls.String),
          Decls.newVar(VAR_PATH, Decls.String),
          Decls.newVar(VAR_ROLE, Decls.String),
          Decls.newVar(VAR_OP, Decls.String));

  public static final List<Object> COMMIT_LOG_TYPES = Collections.singletonList(CommitMeta.class);

  public static final List<Object> REFERENCES_TYPES =
      ImmutableList.of(CommitMeta.class, ReferenceMetadata.class, Reference.class);

  public static final CommitMeta EMPTY_COMMIT_META = CommitMeta.fromMessage("");
  public static final ReferenceMetadata EMPTY_REFERENCE_METADATA =
      ImmutableReferenceMetadata.builder().commitMetaOfHEAD(EMPTY_COMMIT_META).build();
}
