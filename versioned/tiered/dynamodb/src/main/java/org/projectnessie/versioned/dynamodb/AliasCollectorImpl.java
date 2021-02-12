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
package org.projectnessie.versioned.dynamodb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import org.projectnessie.versioned.impl.condition.AliasCollector;
import org.projectnessie.versioned.store.Entity;

import com.google.common.collect.ImmutableSet;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * A tool for canonicalizing an expression before sending to DynamoDb. Responsible for replacing path names and values
 * with aliases where needed. Should be used once per expression.
 */
public class AliasCollectorImpl implements AliasCollector {

  private static final Pattern CLEAN_NAME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9]*$");

  private int counter = 0;
  private final Map<String, AttributeValue> attributeValues = new HashMap<>();
  private final Map<String, String> attributeNames = new HashMap<>();

  int getNext() {
    return counter++;
  }

  public Map<String, AttributeValue> getAttributesValues() {
    return Collections.unmodifiableMap(attributeValues);
  }

  public Map<String, String> getAttributeNames() {
    return Collections.unmodifiableMap(attributeNames);
  }

  /**
   * Escape a path string based on reserved words and naming rules for DynamoDb.
   *
   * <p>If the string is escaped, the mapping is recorded in the collectors list of attribute names for later use.
   *
   * @param unescaped A path segment to escape.
   * @return The original segment (or escaped if certain rules are matched).
   */
  public String escape(String unescaped) {
    // If an attribute name begins with a number or contains a space, a special character, or a reserved word,
    // you must use an expression attribute name to replace that attribute's name in the expression.
    if (RESERVED_WORDS.contains(unescaped.toUpperCase(Locale.US))) {
      String escaped = "#" + unescaped + "XX";
      attributeNames.put(escaped, unescaped);
      return escaped;
    }

    if (CLEAN_NAME.matcher(unescaped).matches()) {
      return unescaped;
    }

    String escaped = "#f" + counter++;
    attributeNames.put(escaped, unescaped);
    return escaped;
  }

  /**
   * Apply these aliases to the attached builder.
   * @param builder Builder to supplement
   * @return The updated builder.
   */
  public PutItemRequest.Builder apply(PutItemRequest.Builder builder) {
    if (!attributeValues.isEmpty()) {
      builder.expressionAttributeValues(Collections.unmodifiableMap(attributeValues));
    }

    if (!attributeNames.isEmpty()) {
      builder.expressionAttributeNames(Collections.unmodifiableMap(attributeNames));
    }
    return builder;
  }

  /**
   * Apply these aliases to the attached builder.
   * @param builder Builder to supplement
   * @return The updated builder.
   */
  public UpdateItemRequest.Builder apply(UpdateItemRequest.Builder builder) {
    if (!attributeValues.isEmpty()) {
      builder.expressionAttributeValues(Collections.unmodifiableMap(attributeValues));
    }

    if (!attributeNames.isEmpty()) {
      builder.expressionAttributeNames(Collections.unmodifiableMap(attributeNames));
    }
    return builder;
  }

  /**
   * Apply these aliases to the attached builder.
   * @param builder Builder to supplement
   * @return The updated builder.
   */
  public DeleteItemRequest.Builder apply(DeleteItemRequest.Builder builder) {
    if (!attributeValues.isEmpty()) {
      builder.expressionAttributeValues(Collections.unmodifiableMap(attributeValues));
    }

    if (!attributeNames.isEmpty()) {
      builder.expressionAttributeNames(Collections.unmodifiableMap(attributeNames));
    }
    return builder;
  }

  /**
   * Create an alias for an attribute value. Records the value locally and returns an alias.
   * @param value The value to be held.
   * @return The name used to reference the aliased value.
   */
  public String alias(Entity value) {
    String escaped = ":v" + counter;
    attributeValues.put(escaped, AttributeValueUtil.fromEntity(value));
    counter++;
    return escaped;
  }

  // straight from https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
  private static final ImmutableSet<String> RESERVED_WORDS = ImmutableSet.of("ABORT", "ABSOLUTE", "ACTION", "ADD",
      "AFTER", "AGENT", "AGGREGATE", "ALL", "ALLOCATE", "ALTER", "ANALYZE", "AND", "ANY", "ARCHIVE", "ARE", "ARRAY",
      "AS", "ASC", "ASCII", "ASENSITIVE", "ASSERTION", "ASYMMETRIC", "AT", "ATOMIC", "ATTACH", "ATTRIBUTE", "AUTH",
      "AUTHORIZATION", "AUTHORIZE", "AUTO", "AVG", "BACK", "BACKUP", "BASE", "BATCH", "BEFORE", "BEGIN", "BETWEEN",
      "BIGINT", "BINARY", "BIT", "BLOB", "BLOCK", "BOOLEAN", "BOTH", "BREADTH", "BUCKET", "BULK", "BY", "BYTE", "CALL",
      "CALLED", "CALLING", "CAPACITY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CHAR", "CHARACTER", "CHECK",
      "CLASS", "CLOB", "CLOSE", "CLUSTER", "CLUSTERED", "CLUSTERING", "CLUSTERS", "COALESCE", "COLLATE", "COLLATION",
      "COLLECTION", "COLUMN", "COLUMNS", "COMBINE", "COMMENT", "COMMIT", "COMPACT", "COMPILE", "COMPRESS", "CONDITION",
      "CONFLICT", "CONNECT", "CONNECTION", "CONSISTENCY", "CONSISTENT", "CONSTRAINT", "CONSTRAINTS", "CONSTRUCTOR",
      "CONSUMED", "CONTINUE", "CONVERT", "COPY", "CORRESPONDING", "COUNT", "COUNTER", "CREATE", "CROSS", "CUBE",
      "CURRENT", "CURSOR", "CYCLE", "DATA", "DATABASE", "DATE", "DATETIME", "DAY", "DEALLOCATE", "DEC", "DECIMAL",
      "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DEFINE", "DEFINED", "DEFINITION", "DELETE", "DELIMITED", "DEPTH",
      "DEREF", "DESC", "DESCRIBE", "DESCRIPTOR", "DETACH", "DETERMINISTIC", "DIAGNOSTICS", "DIRECTORIES", "DISABLE",
      "DISCONNECT", "DISTINCT", "DISTRIBUTE", "DO", "DOMAIN", "DOUBLE", "DROP", "DUMP", "DURATION", "DYNAMIC", "EACH",
      "ELEMENT", "ELSE", "ELSEIF", "EMPTY", "ENABLE", "END", "EQUAL", "EQUALS", "ERROR", "ESCAPE", "ESCAPED", "EVAL",
      "EVALUATE", "EXCEEDED", "EXCEPT", "EXCEPTION", "EXCEPTIONS", "EXCLUSIVE", "EXEC", "EXECUTE", "EXISTS", "EXIT",
      "EXPLAIN", "EXPLODE", "EXPORT", "EXPRESSION", "EXTENDED", "EXTERNAL", "EXTRACT", "FAIL", "FALSE", "FAMILY",
      "FETCH", "FIELDS", "FILE", "FILTER", "FILTERING", "FINAL", "FINISH", "FIRST", "FIXED", "FLATTERN", "FLOAT", "FOR",
      "FORCE", "FOREIGN", "FORMAT", "FORWARD", "FOUND", "FREE", "FROM", "FULL", "FUNCTION", "FUNCTIONS", "GENERAL",
      "GENERATE", "GET", "GLOB", "GLOBAL", "GO", "GOTO", "GRANT", "GREATER", "GROUP", "GROUPING", "HANDLER", "HASH",
      "HAVE", "HAVING", "HEAP", "HIDDEN", "HOLD", "HOUR", "IDENTIFIED", "IDENTITY", "IF", "IGNORE", "IMMEDIATE",
      "IMPORT", "IN", "INCLUDING", "INCLUSIVE", "INCREMENT", "INCREMENTAL", "INDEX", "INDEXED", "INDEXES", "INDICATOR",
      "INFINITE", "INITIALLY", "INLINE", "INNER", "INNTER", "INOUT", "INPUT", "INSENSITIVE", "INSERT", "INSTEAD", "INT",
      "INTEGER", "INTERSECT", "INTERVAL", "INTO", "INVALIDATE", "IS", "ISOLATION", "ITEM", "ITEMS", "ITERATE", "JOIN",
      "KEY", "KEYS", "LAG", "LANGUAGE", "LARGE", "LAST", "LATERAL", "LEAD", "LEADING", "LEAVE", "LEFT", "LENGTH",
      "LESS", "LEVEL", "LIKE", "LIMIT", "LIMITED", "LINES", "LIST", "LOAD", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP",
      "LOCATION", "LOCATOR", "LOCK", "LOCKS", "LOG", "LOGED", "LONG", "LOOP", "LOWER", "MAP", "MATCH", "MATERIALIZED",
      "MAX", "MAXLEN", "MEMBER", "MERGE", "METHOD", "METRICS", "MIN", "MINUS", "MINUTE", "MISSING", "MOD", "MODE",
      "MODIFIES", "MODIFY", "MODULE", "MONTH", "MULTI", "MULTISET", "NAME", "NAMES", "NATIONAL", "NATURAL", "NCHAR",
      "NCLOB", "NEW", "NEXT", "NO", "NONE", "NOT", "NULL", "NULLIF", "NUMBER", "NUMERIC", "OBJECT", "OF", "OFFLINE",
      "OFFSET", "OLD", "ON", "ONLINE", "ONLY", "OPAQUE", "OPEN", "OPERATOR", "OPTION", "OR", "ORDER", "ORDINALITY",
      "OTHER", "OTHERS", "OUT", "OUTER", "OUTPUT", "OVER", "OVERLAPS", "OVERRIDE", "OWNER", "PAD", "PARALLEL",
      "PARAMETER", "PARAMETERS", "PARTIAL", "PARTITION", "PARTITIONED", "PARTITIONS", "PATH", "PERCENT", "PERCENTILE",
      "PERMISSION", "PERMISSIONS", "PIPE", "PIPELINED", "PLAN", "POOL", "POSITION", "PRECISION", "PREPARE", "PRESERVE",
      "PRIMARY", "PRIOR", "PRIVATE", "PRIVILEGES", "PROCEDURE", "PROCESSED", "PROJECT", "PROJECTION", "PROPERTY",
      "PROVISIONING", "PUBLIC", "PUT", "QUERY", "QUIT", "QUORUM", "RAISE", "RANDOM", "RANGE", "RANK", "RAW", "READ",
      "READS", "REAL", "REBUILD", "RECORD", "RECURSIVE", "REDUCE", "REF", "REFERENCE", "REFERENCES", "REFERENCING",
      "REGEXP", "REGION", "REINDEX", "RELATIVE", "RELEASE", "REMAINDER", "RENAME", "REPEAT", "REPLACE", "REQUEST",
      "RESET", "RESIGNAL", "RESOURCE", "RESPONSE", "RESTORE", "RESTRICT", "RESULT", "RETURN", "RETURNING", "RETURNS",
      "REVERSE", "REVOKE", "RIGHT", "ROLE", "ROLES", "ROLLBACK", "ROLLUP", "ROUTINE", "ROW", "ROWS", "RULE", "RULES",
      "SAMPLE", "SATISFIES", "SAVE", "SAVEPOINT", "SCAN", "SCHEMA", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SECTION",
      "SEGMENT", "SEGMENTS", "SELECT", "SELF", "SEMI", "SENSITIVE", "SEPARATE", "SEQUENCE", "SERIALIZABLE", "SESSION",
      "SET", "SETS", "SHARD", "SHARE", "SHARED", "SHORT", "SHOW", "SIGNAL", "SIMILAR", "SIZE", "SKEWED", "SMALLINT",
      "SNAPSHOT", "SOME", "SOURCE", "SPACE", "SPACES", "SPARSE", "SPECIFIC", "SPECIFICTYPE", "SPLIT", "SQL", "SQLCODE",
      "SQLERROR", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "START", "STATE", "STATIC", "STATUS", "STORAGE", "STORE",
      "STORED", "STREAM", "STRING", "STRUCT", "STYLE", "SUB", "SUBMULTISET", "SUBPARTITION", "SUBSTRING", "SUBTYPE",
      "SUM", "SUPER", "SYMMETRIC", "SYNONYM", "SYSTEM", "TABLE", "TABLESAMPLE", "TEMP", "TEMPORARY", "TERMINATED",
      "TEXT", "THAN", "THEN", "THROUGHPUT", "TIME", "TIMESTAMP", "TIMEZONE", "TINYINT", "TO", "TOKEN", "TOTAL", "TOUCH",
      "TRAILING", "TRANSACTION", "TRANSFORM", "TRANSLATE", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE",
      "TRUNCATE", "TTL", "TUPLE", "TYPE", "UNDER", "UNDO", "UNION", "UNIQUE", "UNIT", "UNKNOWN", "UNLOGGED", "UNNEST",
      "UNPROCESSED", "UNSIGNED", "UNTIL", "UPDATE", "UPPER", "URL", "USAGE", "USE", "USER", "USERS", "USING", "UUID",
      "VACUUM", "VALUE", "VALUED", "VALUES", "VARCHAR", "VARIABLE", "VARIANCE", "VARINT", "VARYING", "VIEW", "VIEWS",
      "VIRTUAL", "VOID", "WAIT", "WHEN", "WHENEVER", "WHERE", "WHILE", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK",
      "WRAPPED", "WRITE", "YEAR", "ZONE");
}
