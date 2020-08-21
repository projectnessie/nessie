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
package com.dremio.nessie.versioned.impl.condition;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.immutables.value.Value.Immutable;

import com.dremio.nessie.versioned.impl.condition.AliasCollector.Aliasable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

@Immutable
public abstract class ExpressionPath implements Value {

  public abstract NameSegment getRoot();

  @Override
  public String toString() {
    return "ExpressionPath [" + asString() + "]";
  }

  /**
   * Return this path as a Dynamo expression string.
   *
   * @return The expression string.
   */
  public String asString() {
    StringBuilder sb = new StringBuilder();
    getRoot().accept(new PathVisitor<Boolean, Void, RuntimeException>() {

      @Override
      public Void visitName(NameSegment segment, Boolean first) throws RuntimeException {
        if (!first) {
          sb.append(".");
        }
        sb.append(segment.getName());
        return childOrNull(segment);
      }

      private Void childOrNull(PathSegment segment) {
        segment.getChild().ifPresent(c -> c.accept(this,  false));
        return null;
      }

      @Override
      public Void visitPosition(PositionSegment segment, Boolean first) throws RuntimeException {
        Preconditions.checkArgument(!first);
        sb.append("[");
        sb.append(segment.getPosition());
        sb.append("]");
        return childOrNull(segment);
      }
    }, true);
    return sb.toString();
  }

  public interface PathVisitor<IN, OUT, EX extends Exception> {

    OUT visitName(NameSegment segment, IN in) throws EX;

    OUT visitPosition(PositionSegment segment, IN in) throws EX;

  }

  public abstract static class PathSegment implements Aliasable<PathSegment> {

    public static class Builder {
      private final int builderOrdinal;
      private final String name;
      private final Integer position;
      private final ExpressionPath.Builder builder;

      private Builder(int builderOrdinal, String path, Integer position, ExpressionPath.Builder builder) {
        super();
        Preconditions.checkArgument((position == null && path != null) || (position != null && path == null),
            "Only one of position or path can be non-null.");
        this.builderOrdinal = builderOrdinal;
        this.name = path;
        this.position = position;
        this.builder = builder;
      }

      public ExpressionPath build() {
        return builder.build(builderOrdinal);
      }

      /**
       * Add a name segment to this path.
       * @param name The name to add.
       * @return The builder that includes this segment.
       */
      public PathSegment.Builder name(String name) {
        PathSegment.Builder subSegment = new PathSegment.Builder(builderOrdinal + 1, name, null, builder);
        builder.segments.add(subSegment);
        return subSegment;
      }

      /**
       * Add a positional segment to this path.
       * @param position The position to add.
       * @return The builder that includes this segment.
       */
      public PathSegment.Builder position(int position) {
        PathSegment.Builder subSegment = new PathSegment.Builder(builderOrdinal + 1, null, position, builder);
        builder.segments.add(subSegment);
        return subSegment;
      }
    }

    public abstract <IN, OUT, EX extends Exception> OUT accept(PathVisitor<IN, OUT, EX> visitor, IN in) throws EX;

    public abstract Optional<PathSegment> getChild();

    public NameSegment asName() {
      return (NameSegment) this;
    }

    public PositionSegment asPosition() {
      return (PositionSegment) this;
    }

    public boolean isName() {
      return this instanceof NameSegment;
    }

    public boolean isPosition() {
      return this instanceof PositionSegment;
    }

  }


  @Override
  public ExpressionPath alias(AliasCollector c) {
    return ImmutableExpressionPath.builder().root((NameSegment) getRoot().alias(c)).build();
  }

  @Immutable
  public abstract static class PositionSegment extends PathSegment {
    public abstract int getPosition();


    @Override
    public PathSegment alias(AliasCollector c) {
      return ImmutablePositionSegment.builder().position(getPosition()).child(getChild().map(p -> p.alias(c))).build();
    }


    @Override
    public <IN, OUT, EX extends Exception> OUT accept(PathVisitor<IN, OUT, EX> visitor, IN in) throws EX {
      return visitor.visitPosition(this, in);
    }
  }

  public PathSegment.Builder toBuilder() {
    return Builder.toBuilder(this);
  }

  @Immutable
  public abstract static class NameSegment extends PathSegment {
    public abstract String getName();

    @Override
    public PathSegment alias(AliasCollector c) {
      return ImmutableNameSegment.builder().name(c.escape(getName())).child(getChild().map(p -> p.alias(c))).build();
    }

    @Override
    public <IN, OUT, EX extends Exception> OUT accept(PathVisitor<IN, OUT, EX> visitor, IN in) throws EX {
      return visitor.visitName(this, in);
    }
  }

  /**
   * Construct a builder that creates a complete ExpressionPath.
   * @param initialPathName The path name at the root of the path.
   * @return The builder
   */
  public static PathSegment.Builder builder(String initialPathName) {
    Builder b = new Builder();
    PathSegment.Builder seg0 = new PathSegment.Builder(0, initialPathName, null, b);
    b.segments.add(seg0);
    return seg0;
  }

  private static class Builder {

    private final List<PathSegment.Builder> segments = new ArrayList<>();

    private Builder() {
    }

    private static PathSegment.Builder toBuilder(ExpressionPath path) {
      PathSegment seg = path.getRoot();

      int i = 0;
      Builder b = new Builder();
      while (seg != null) {
        b.segments.add(new PathSegment.Builder(
            i,
            seg.isName() ? seg.asName().getName() : null,
            seg.isPosition() ? seg.asPosition().getPosition() : null,
            b));
        seg = seg.getChild().orElse(null);
        i++;
      }

      return b.segments.get(b.segments.size() - 1);
    }

    private ExpressionPath build(int builderOrdinal) {
      Preconditions.checkArgument(builderOrdinal == segments.size() - 1, "You can only call build on the final segment of your path.");
      Preconditions.checkArgument(!segments.isEmpty(), "At least one segment must be defined.");
      List<PathSegment.Builder> reversed = Lists.reverse(segments);
      PathSegment segment = null;
      for (PathSegment.Builder seg : reversed) {
        if (seg.name != null) {
          segment = ImmutableNameSegment.builder().child(Optional.ofNullable(segment)).name(seg.name).build();
        } else {
          segment = ImmutablePositionSegment.builder().child(Optional.ofNullable(segment)).position(seg.position).build();
          Preconditions.checkArgument(seg.builderOrdinal != 0);
        }
      }

      return ImmutableExpressionPath.builder().root((NameSegment) segment).build();
    }
  }


  @Override
  public Type getType() {
    return Type.PATH;
  }

  @Override
  public ExpressionPath getPath() {
    return this;
  }

}
