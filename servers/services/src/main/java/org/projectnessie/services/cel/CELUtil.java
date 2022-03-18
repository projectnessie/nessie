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
import java.util.List;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.types.jackson.JacksonRegistry;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.RefLogResponse;
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
  public static final String VAR_OPERATIONS = "operations";
  public static final String VAR_REFLOG = "reflog";

  public static final List<Decl> REFERENCES_DECLARATIONS =
      ImmutableList.of(
          Decls.newVar(VAR_COMMIT, Decls.newObjectType(CommitMeta.class.getName())),
          Decls.newVar(VAR_REF, Decls.newObjectType(Reference.class.getName())),
          Decls.newVar(VAR_REF_META, Decls.newObjectType(ReferenceMetadata.class.getName())),
          Decls.newVar(VAR_REF_TYPE, Decls.String));

  public static final List<Decl> COMMIT_LOG_DECLARATIONS =
      ImmutableList.of(
          Decls.newVar(VAR_COMMIT, Decls.newObjectType(CommitMeta.class.getName())),
          Decls.newVar(
              VAR_OPERATIONS,
              Decls.newListType(Decls.newObjectType(OperationForCel.class.getName()))));

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

  public static final List<Object> COMMIT_LOG_TYPES =
      ImmutableList.of(CommitMeta.class, OperationForCel.class, ContentKey.class, Namespace.class);

  public static final List<Object> REFLOG_TYPES =
      ImmutableList.of(RefLogResponse.RefLogResponseEntry.class);

  public static final List<Object> REFERENCES_TYPES =
      ImmutableList.of(CommitMeta.class, ReferenceMetadata.class, Reference.class);

  public static final CommitMeta EMPTY_COMMIT_META = CommitMeta.fromMessage("");
  public static final ReferenceMetadata EMPTY_REFERENCE_METADATA =
      ImmutableReferenceMetadata.builder().commitMetaOfHEAD(EMPTY_COMMIT_META).build();

  public static final List<Decl> REFLOG_DECLARATIONS =
      ImmutableList.of(
          Decls.newVar(
              VAR_REFLOG, Decls.newObjectType(RefLogResponse.RefLogResponseEntry.class.getName())));

  /**
   * 'Mirrored' interface wrapping a {@link Operation} for CEL to have convenience fields for CEL
   * and to avoid missing fields due to {@code @JsonIgnore}.
   */
  @SuppressWarnings("unused")
  public interface OperationForCel {
    String getType();

    List<String> getKeyElements();

    String getKey();

    List<String> getNamespaceElements();

    String getName();

    String getNamespace();

    ContentForCel getContent();
  }

  /**
   * 'Mirrored' interface wrapping a {@link Content} for CEL to have convenience fields for CEL and
   * to avoid missing fields due to {@code @JsonIgnore}.
   */
  @SuppressWarnings("unused")
  public interface ContentForCel {
    String getType();

    String getId();
  }

  /**
   * 'Mirrors' Nessie model objects for CEL.
   *
   * @param model Nessie model object
   * @return object suitable for CEL expressions
   */
  public static Object forCel(Object model) {
    if (model instanceof Content) {
      Content c = (Content) model;
      return new ContentForCel() {
        @Override
        public String getType() {
          return c.getType().name();
        }

        @Override
        public String getId() {
          return c.getId();
        }
      };
    }
    if (model instanceof Operation) {
      Operation op = (Operation) model;
      return new OperationForCel() {
        @Override
        public String getType() {
          if (op instanceof Put) {
            return "PUT";
          }
          if (op instanceof Delete) {
            return "DELETE";
          }
          return "OPERATION";
        }

        @Override
        public List<String> getKeyElements() {
          return op.getKey().getElements();
        }

        @Override
        public String getKey() {
          return op.getKey().toString();
        }

        @Override
        public String getNamespace() {
          return op.getKey().getNamespace().name();
        }

        @Override
        public List<String> getNamespaceElements() {
          return op.getKey().getNamespace().getElements();
        }

        @Override
        public String getName() {
          return op.getKey().getName();
        }

        @Override
        public ContentForCel getContent() {
          if (op instanceof Put) {
            return (ContentForCel) forCel(((Put) op).getContent());
          }
          return null;
        }

        @Override
        public String toString() {
          return op.toString();
        }
      };
    }
    return model;
  }
}
