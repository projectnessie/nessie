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
package org.projectnessie.versioned.storage.serialize;

import static java.util.Collections.emptyList;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;
import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uniqueId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.nessie.relocated.protobuf.InvalidProtocolBufferException;
import org.projectnessie.nessie.relocated.protobuf.Parser;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.objtypes.RefObj;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.objtypes.TagObj;
import org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj;
import org.projectnessie.versioned.storage.common.persist.ImmutableReference;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.ObjTypes;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.persist.UpdateableObj;
import org.projectnessie.versioned.storage.common.proto.StorageTypes;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.CommitProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.CommitTypeProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.CompressionProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.ContentValueProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.CustomProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.HeaderEntry;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.IndexProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.IndexSegmentsProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.ObjProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.RefProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.ReferenceProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.StringProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Stripe;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.TagProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.UniqueIdProto;

public final class ProtoSerialization {

  private ProtoSerialization() {}

  public static byte[] serializeReference(Reference reference) {
    if (reference == null) {
      return null;
    }
    ReferenceProto.Builder refBuilder =
        ReferenceProto.newBuilder()
            .setName(reference.name())
            .setPointer(serializeObjId(reference.pointer()))
            .setDeleted(reference.deleted());
    long created = reference.createdAtMicros();
    if (created != 0L) {
      refBuilder.setCreatedAtMicros(created);
    }
    ObjId extendedInfoObj = reference.extendedInfoObj();
    if (extendedInfoObj != null) {
      refBuilder.setExtendedInfoObj(serializeObjId(extendedInfoObj));
    }
    for (Reference.PreviousPointer previousPointer : reference.previousPointers()) {
      refBuilder.addPreviousPointers(
          StorageTypes.ReferencePreviousProto.newBuilder()
              .setPointer(serializeObjId(previousPointer.pointer()))
              .setTimestamp(previousPointer.timestamp()));
    }
    return refBuilder.build().toByteArray();
  }

  public static Reference deserializeReference(byte[] reference) {
    if (reference == null) {
      return null;
    }
    try {
      ReferenceProto proto = ReferenceProto.parseFrom(reference);
      ImmutableReference.Builder ref =
          ImmutableReference.builder()
              .name(proto.getName())
              .pointer(deserializeObjId(proto.getPointer()))
              .deleted(proto.getDeleted())
              .createdAtMicros(proto.getCreatedAtMicros())
              .extendedInfoObj(deserializeObjId(proto.getExtendedInfoObj()));
      for (StorageTypes.ReferencePreviousProto previousProto : proto.getPreviousPointersList()) {
        ref.addPreviousPointers(
            Reference.PreviousPointer.previousPointer(
                deserializeObjId(previousProto.getPointer()), previousProto.getTimestamp()));
      }
      return ref.build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] serializePreviousPointers(List<Reference.PreviousPointer> previousPointers) {
    if (previousPointers == null || previousPointers.isEmpty()) {
      return null;
    }
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      for (Reference.PreviousPointer prev : previousPointers) {
        StorageTypes.ReferencePreviousProto.newBuilder()
            .setPointer(serializeObjId(prev.pointer()))
            .setTimestamp(prev.timestamp())
            .build()
            .writeDelimitedTo(output);
      }
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Reference.PreviousPointer> deserializePreviousPointers(
      byte[] previousPointers) {
    if (previousPointers == null || previousPointers.length == 0) {
      return emptyList();
    }

    try {
      ByteArrayInputStream input = new ByteArrayInputStream(previousPointers);
      Parser<StorageTypes.ReferencePreviousProto> parser =
          StorageTypes.ReferencePreviousProto.parser();
      List<Reference.PreviousPointer> r = new ArrayList<>();
      while (input.available() > 0) {
        StorageTypes.ReferencePreviousProto proto = parser.parseDelimitedFrom(input);
        Reference.PreviousPointer previousPointer =
            Reference.PreviousPointer.previousPointer(
                deserializeObjId(proto.getPointer()), proto.getTimestamp());
        r.add(previousPointer);
      }
      return r;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static ByteString serializeObjId(ObjId id) {
    if (id == null) {
      return null;
    }
    return id.asBytes();
  }

  public static ObjId deserializeObjId(ByteString bytes) {
    if (bytes == null || bytes.isEmpty()) {
      return null;
    }
    return ObjId.objIdFromBytes(bytes);
  }

  public static void serializeObjIds(List<ObjId> ids, Consumer<ByteString> receiver) {
    if (ids != null) {
      for (ObjId id : ids) {
        receiver.accept(serializeObjId(id));
      }
    }
  }

  public static void deserializeObjIds(List<ByteString> bytes, Consumer<ObjId> receiver) {
    if (bytes != null) {
      for (ByteString b : bytes) {
        receiver.accept(deserializeObjId(b));
      }
    }
  }

  public static List<ObjId> deserializeObjIds(List<ByteString> predecessorsList) {
    List<ObjId> result = new ArrayList<>(predecessorsList.size());
    deserializeObjIds(predecessorsList, result::add);
    return result;
  }

  public static byte[] serializeObj(
      Obj obj, int incrementalIndexSizeLimit, int indexSizeLimit, boolean includeVersionToken)
      throws ObjTooLargeException {
    if (obj == null) {
      return null;
    }
    ObjProto.Builder b = ObjProto.newBuilder();
    if (obj.type() instanceof StandardObjType) {
      switch (((StandardObjType) obj.type())) {
        case COMMIT:
          return b.setCommit(serializeCommit((CommitObj) obj, incrementalIndexSizeLimit))
              .build()
              .toByteArray();
        case VALUE:
          return b.setContentValue(serializeContentValue((ContentValueObj) obj))
              .build()
              .toByteArray();
        case REF:
          return b.setRef(serializeRef((RefObj) obj)).build().toByteArray();
        case INDEX_SEGMENTS:
          return b.setIndexSegments(serializeIndexSegments((IndexSegmentsObj) obj))
              .build()
              .toByteArray();
        case INDEX:
          return b.setIndex(serializeIndex((IndexObj) obj, indexSizeLimit)).build().toByteArray();
        case STRING:
          return b.setStringData(serializeStringData((StringObj) obj)).build().toByteArray();
        case TAG:
          return b.setTag(serializeTag((TagObj) obj)).build().toByteArray();
        case UNIQUE:
          return b.setUniqueId(serializeUniqueId((UniqueIdObj) obj)).build().toByteArray();
        default:
          throw new UnsupportedOperationException("Unknown standard object type " + obj.type());
      }
    } else {
      return b.setCustom(serializeCustom(obj, includeVersionToken)).build().toByteArray();
    }
  }

  public static Obj deserializeObj(ObjId id, ByteBuffer serialized, String versionToken) {
    if (serialized == null) {
      return null;
    }
    try {
      ObjProto obj = ObjProto.parseFrom(serialized);
      return deserializeObjProto(id, obj, versionToken);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static Obj deserializeObj(ObjId id, byte[] serialized, String versionToken) {
    if (serialized == null) {
      return null;
    }
    try {
      ObjProto obj = ObjProto.parseFrom(serialized);
      return deserializeObjProto(id, obj, versionToken);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static Obj deserializeObjProto(ObjId id, ObjProto obj, String versionToken) {
    if (obj.hasCommit()) {
      return deserializeCommit(id, obj.getCommit());
    }
    if (obj.hasContentValue()) {
      return deserializeContentValue(id, obj.getContentValue());
    }
    if (obj.hasRef()) {
      return deserializeRef(id, obj.getRef());
    }
    if (obj.hasIndexSegments()) {
      return deserializeIndexSegments(id, obj.getIndexSegments());
    }
    if (obj.hasIndex()) {
      return deserializeIndex(id, obj.getIndex());
    }
    if (obj.hasStringData()) {
      return deserializeStringData(id, obj.getStringData());
    }
    if (obj.hasTag()) {
      return deserializeTag(id, obj.getTag());
    }
    if (obj.hasUniqueId()) {
      return deserializeUniqueId(id, obj.getUniqueId());
    }
    if (obj.hasCustom()) {
      return deserializeCustom(
          id,
          obj.getCustom(),
          versionToken != null ? versionToken : obj.getCustom().getVersionToken());
    }
    throw new UnsupportedOperationException("Cannot deserialize " + obj);
  }

  private static CommitObj deserializeCommit(ObjId id, CommitProto commit) {
    CommitObj.Builder b =
        commitBuilder()
            .id(id)
            .created(commit.getCreated())
            .seq(commit.getSeq())
            .message(commit.getMessage())
            .incrementalIndex(commit.getIncrementalIndex())
            .incompleteIndex(commit.getIncompleteIndex())
            .commitType(CommitType.valueOf(commit.getCommitType().name()));
    deserializeObjIds(commit.getTailList(), b::addTail);
    deserializeObjIds(commit.getSecondaryParentsList(), b::addSecondaryParents);
    CommitHeaders.Builder h = CommitHeaders.newCommitHeaders();
    for (HeaderEntry e : commit.getHeadersList()) {
      for (String v : e.getValuesList()) {
        h.add(e.getName(), v);
      }
    }
    b.headers(h.build());
    if (commit.hasReferenceIndex()) {
      b.referenceIndex(deserializeObjId(commit.getReferenceIndex()));
    }
    for (Stripe s : commit.getReferenceIndexStripesList()) {
      b.addReferenceIndexStripes(
          indexStripe(
              keyFromString(s.getFirstKey()),
              keyFromString(s.getLastKey()),
              deserializeObjId(s.getSegment())));
    }
    return b.build();
  }

  private static CommitProto.Builder serializeCommit(CommitObj obj, int indexSizeLimit)
      throws ObjTooLargeException {
    CommitProto.Builder b =
        CommitProto.newBuilder()
            .setCreated(obj.created())
            .setSeq(obj.seq())
            .setMessage(obj.message())
            .setIncrementalIndex(verifySize(obj.incrementalIndex(), indexSizeLimit))
            .setIncompleteIndex(obj.incompleteIndex())
            .setCommitType(CommitTypeProto.valueOf(obj.commitType().name()));
    serializeObjIds(obj.tail(), b::addTail);
    serializeObjIds(obj.secondaryParents(), b::addSecondaryParents);
    for (String h : obj.headers().keySet()) {
      b.addHeaders(HeaderEntry.newBuilder().setName(h).addAllValues(obj.headers().getAll(h)));
    }
    ObjId referenceIndexId = obj.referenceIndex();
    if (referenceIndexId != null) {
      b.setReferenceIndex(serializeObjId(referenceIndexId));
    }
    for (IndexStripe indexStripe : obj.referenceIndexStripes()) {
      b.addReferenceIndexStripes(
          Stripe.newBuilder()
              .setFirstKey(indexStripe.firstKey().rawString())
              .setLastKey(indexStripe.lastKey().rawString())
              .setSegment(serializeObjId(indexStripe.segment())));
    }
    return b;
  }

  private static ByteString verifySize(ByteString index, int indexSizeLimit)
      throws ObjTooLargeException {
    int size = index.size();
    if (size > indexSizeLimit) {
      throw new ObjTooLargeException(size, indexSizeLimit);
    }
    return index;
  }

  private static ContentValueObj deserializeContentValue(ObjId id, ContentValueProto contentValue) {
    return contentValue(
        id, contentValue.getContentId(), contentValue.getPayload(), contentValue.getData());
  }

  private static ContentValueProto.Builder serializeContentValue(ContentValueObj obj) {
    return ContentValueProto.newBuilder()
        .setContentId(obj.contentId())
        .setPayload(obj.payload())
        .setData(obj.data());
  }

  private static RefObj deserializeRef(ObjId id, RefProto ref) {
    return ref(
        id,
        ref.getName(),
        deserializeObjId(ref.getInitialPointer()),
        ref.getCreatedAtMicros(),
        deserializeObjId(ref.getExtendedInfoObj()));
  }

  private static RefProto.Builder serializeRef(RefObj obj) {
    RefProto.Builder refBuilder =
        RefProto.newBuilder()
            .setName(obj.name())
            .setCreatedAtMicros(obj.createdAtMicros())
            .setInitialPointer(serializeObjId(obj.initialPointer()));
    ObjId extendedInfoObj = obj.extendedInfoObj();
    if (extendedInfoObj != null) {
      refBuilder.setExtendedInfoObj(serializeObjId(extendedInfoObj));
    }
    return refBuilder;
  }

  private static IndexSegmentsObj deserializeIndexSegments(
      ObjId id, IndexSegmentsProto indexSegments) {
    List<IndexStripe> stripes = new ArrayList<>(indexSegments.getStripesCount());
    for (Stripe s : indexSegments.getStripesList()) {
      stripes.add(
          indexStripe(
              keyFromString(s.getFirstKey()),
              keyFromString(s.getLastKey()),
              deserializeObjId(s.getSegment())));
    }
    return indexSegments(id, stripes);
  }

  private static IndexSegmentsProto.Builder serializeIndexSegments(IndexSegmentsObj obj) {
    IndexSegmentsProto.Builder b = IndexSegmentsProto.newBuilder();
    for (IndexStripe indexStripe : obj.stripes()) {
      b.addStripes(
          Stripe.newBuilder()
              .setFirstKey(indexStripe.firstKey().rawString())
              .setLastKey(indexStripe.lastKey().rawString())
              .setSegment(serializeObjId(indexStripe.segment())));
    }
    return b;
  }

  private static IndexObj deserializeIndex(ObjId id, IndexProto index) {
    return index(id, index.getIndex());
  }

  private static IndexProto.Builder serializeIndex(IndexObj obj, int indexSizeLimit)
      throws ObjTooLargeException {
    return IndexProto.newBuilder().setIndex(verifySize(obj.index(), indexSizeLimit));
  }

  private static StringObj deserializeStringData(ObjId id, StringProto stringData) {
    return stringData(
        id,
        stringData.getContentType(),
        Compression.valueOf(stringData.getCompression().name()),
        stringData.hasFilename() ? stringData.getFilename() : null,
        deserializeObjIds(stringData.getPredecessorsList()),
        stringData.getText());
  }

  private static StringProto.Builder serializeStringData(StringObj obj) {
    StringProto.Builder b =
        StringProto.newBuilder()
            .setCompression(CompressionProto.valueOf(obj.compression().name()))
            .setContentType(obj.contentType())
            .setText(obj.text());
    if (obj.filename() != null) {
      b.setFilename(obj.filename());
    }
    serializeObjIds(obj.predecessors(), b::addPredecessors);
    return b;
  }

  private static TagObj deserializeTag(ObjId id, TagProto tag) {
    CommitHeaders tagHeaders = null;
    if (tag.getHeadersCount() > 0) {
      CommitHeaders.Builder h = CommitHeaders.newCommitHeaders();
      for (HeaderEntry e : tag.getHeadersList()) {
        for (String v : e.getValuesList()) {
          h.add(e.getName(), v);
        }
      }
      tagHeaders = h.build();
    }
    String message = tag.hasMessage() ? tag.getMessage() : null;
    ByteString signature = tag.hasSignature() ? tag.getSignature() : null;
    return tag(id, message, tagHeaders, signature);
  }

  private static TagProto.Builder serializeTag(TagObj obj) {
    TagProto.Builder tag = TagProto.newBuilder();
    if (obj.message() != null) {
      tag.setMessage(obj.message());
    }
    if (obj.signature() != null) {
      tag.setSignature(obj.signature());
    }
    CommitHeaders headers = obj.headers();
    if (headers != null) {
      for (String h : headers.keySet()) {
        tag.addHeaders(HeaderEntry.newBuilder().setName(h).addAllValues(headers.getAll(h)));
      }
    }
    return tag;
  }

  private static UniqueIdObj deserializeUniqueId(ObjId id, UniqueIdProto uniqueId) {
    return uniqueId(id, uniqueId.getSpace(), uniqueId.getValue());
  }

  private static UniqueIdProto.Builder serializeUniqueId(UniqueIdObj obj) {
    return UniqueIdProto.newBuilder().setSpace(obj.space()).setValue(obj.value());
  }

  private static Obj deserializeCustom(ObjId id, CustomProto custom, String versionToken) {
    ObjType type = ObjTypes.forShortName(custom.getObjType());
    if (versionToken == null) {
      // versionToken is set when reading objects from the database, but cache-deserialization has
      // the versionToken in the object
      versionToken = custom.getVersionToken();
    }
    return SmileSerialization.deserializeObj(
        id,
        versionToken,
        custom.getData().toByteArray(),
        type.targetClass(),
        custom.getCompression().name());
  }

  private static CustomProto.Builder serializeCustom(Obj obj, boolean includeVersionToken) {
    CustomProto.Builder builder = CustomProto.newBuilder().setObjType(obj.type().shortName());
    if (obj instanceof UpdateableObj && includeVersionToken) {
      builder.setVersionToken(((UpdateableObj) obj).versionToken());
    }
    byte[] bytes =
        SmileSerialization.serializeObj(
            obj,
            compression -> builder.setCompression(CompressionProto.valueOf(compression.name())));
    builder.setData(ByteString.copyFrom(bytes));
    return builder;
  }
}
