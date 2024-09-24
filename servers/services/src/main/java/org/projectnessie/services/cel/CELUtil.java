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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.relocated.com.google.api.expr.v1alpha1.Decl;
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
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.versioned.KeyEntry;

/** A utility class for CEL declarations and other things. */
public final class CELUtil {

  public static final String CONTAINER = "org.projectnessie.model";
  public static final ScriptHost SCRIPT_HOST =
      ScriptHost.newBuilder().registry(JacksonRegistry.newRegistry()).build();

  public static final String VAR_REF = "ref";
  public static final String VAR_REF_TYPE = "refType";
  public static final String VAR_REF_META = "refMeta";
  public static final String VAR_COMMIT = "commit";
  public static final String VAR_KEY = "key";
  public static final String VAR_ENTRY = "entry";
  public static final String VAR_PATH = "path";
  public static final String VAR_ROLE = "role";
  public static final String VAR_ROLES = "roles";
  public static final String VAR_OP = "op";
  public static final String VAR_ACTIONS = "actions";
  public static final String VAR_API = "api";
  public static final String VAR_OPERATIONS = "operations";
  public static final String VAR_CONTENT_TYPE = "contentType";

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

  public static final List<Decl> CONTENT_KEY_DECLARATIONS =
      ImmutableList.of(
          Decls.newVar(VAR_KEY, Decls.newObjectType(KeyedEntityForCel.class.getName())));

  public static final List<Decl> ENTRIES_DECLARATIONS =
      ImmutableList.of(
          Decls.newVar(VAR_ENTRY, Decls.newObjectType(KeyEntryForCel.class.getName())));

  public static final List<Decl> AUTHORIZATION_RULE_DECLARATIONS =
      ImmutableList.of(
          Decls.newVar(VAR_REF, Decls.String),
          Decls.newVar(VAR_PATH, Decls.String),
          Decls.newVar(VAR_CONTENT_TYPE, Decls.String),
          Decls.newVar(VAR_ROLE, Decls.String),
          Decls.newVar(VAR_ROLES, Decls.newListType(Decls.String)),
          Decls.newVar(VAR_OP, Decls.String));

  public static final List<Object> COMMIT_LOG_TYPES =
      ImmutableList.of(CommitMeta.class, OperationForCel.class, ContentKey.class, Namespace.class);

  public static final List<Object> CONTENT_KEY_TYPES = ImmutableList.of(KeyedEntityForCel.class);

  public static final List<Object> REFERENCES_TYPES =
      ImmutableList.of(CommitMeta.class, ReferenceMetadata.class, Reference.class);

  public static final List<Object> ENTRIES_TYPES = ImmutableList.of(KeyEntryForCel.class);

  public static final CommitMeta EMPTY_COMMIT_META = CommitMeta.fromMessage("");
  public static final ReferenceMetadata EMPTY_REFERENCE_METADATA =
      ImmutableReferenceMetadata.builder().commitMetaOfHEAD(EMPTY_COMMIT_META).build();

  private CELUtil() {}

  /**
   * Base interface for 'mirrored' wrappers exposing data to CEL expression about entities that are
   * associated with keys.
   */
  @SuppressWarnings("unused")
  public interface KeyedEntityForCel {
    List<String> getKeyElements();

    String getKey();

    String getEncodedKey();

    List<String> getNamespaceElements();

    String getName();

    String getNamespace();
  }

  /**
   * 'Mirrored' interface wrapping a {@link Operation} for CEL to have convenience fields for CEL
   * and to avoid missing fields due to {@code @JsonIgnore}.
   */
  @SuppressWarnings("unused")
  public interface OperationForCel extends KeyedEntityForCel {
    String getType();

    ContentForCel getContent();
  }

  /**
   * 'Mirrored' interface wrapping a {@link KeyEntry} for CEL to have convenience fields and
   * maintain backward compatibility to older ways of exposing this data to scripts..
   */
  @SuppressWarnings("unused")
  public interface KeyEntryForCel extends KeyedEntityForCel {
    String getContentType();
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
      return new ContentEntity((Content) model);
    }
    if (model instanceof Operation) {
      return new OperationForCelImpl((Operation) model);
    }
    if (model instanceof ContentKey) {
      return new KeyForCelImpl((ContentKey) model);
    }
    return model;
  }

  public static Object forCel(ContentKey key, Content.Type type) {
    return new KeyEntryForCelImpl(key, type);
  }

  private static class KeyForCelImpl extends AbstractKeyedEntity {
    private final ContentKey contentKey;

    KeyForCelImpl(ContentKey k) {
      this.contentKey = k;
    }

    @Override
    protected ContentKey key() {
      return contentKey;
    }

    @Override
    public String toString() {
      return contentKey.toString();
    }
  }

  /**
   * the class does not wrap a {@link KeyEntry} because we need to be able to evaluate {@link
   * java.util.function.BiPredicate}&lt;ContentKey, Content.Type&gt; as early as possible to avoid
   * redundant work.
   */
  private static class KeyEntryForCelImpl extends AbstractKeyedEntity implements KeyEntryForCel {
    private final ContentKey key;
    private final Content.Type type;

    private KeyEntryForCelImpl(ContentKey key, Content.Type type) {
      this.key = key;
      this.type = type;
    }

    @Override
    protected ContentKey key() {
      return key;
    }

    @Override
    public String getContentType() {
      return type.name();
    }

    @Override
    public String toString() {
      return "KeyEntryForCelImpl{" + "key=" + key + ", type=" + type + "}";
    }
  }

  private static class ContentEntity implements ContentForCel {
    private final Content content;

    private ContentEntity(Content content) {
      this.content = content;
    }

    @Override
    public String getType() {
      return content.getType().name();
    }

    @Override
    public String getId() {
      return content.getId();
    }
  }

  private static class OperationForCelImpl extends AbstractKeyedEntity implements OperationForCel {
    private final Operation op;

    OperationForCelImpl(Operation op) {
      this.op = op;
    }

    @Override
    protected ContentKey key() {
      return op.getKey();
    }

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
  }

  private abstract static class AbstractKeyedEntity implements KeyedEntityForCel {
    protected abstract ContentKey key();

    @Override
    public List<String> getKeyElements() {
      return key().getElements();
    }

    @Override
    public String getKey() {
      return key().toString();
    }

    @Override
    public String getEncodedKey() {
      return key().toPathString();
    }

    @Override
    public String getNamespace() {
      return key().getNamespace().toPathString();
    }

    @Override
    public List<String> getNamespaceElements() {
      return key().getNamespace().getElements();
    }

    @Override
    public String getName() {
      return key().getName();
    }
  }
}
