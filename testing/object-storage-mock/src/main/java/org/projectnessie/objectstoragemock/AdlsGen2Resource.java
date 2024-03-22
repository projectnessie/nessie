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
package org.projectnessie.objectstoragemock;

import static com.google.common.net.HttpHeaders.CONTENT_MD5;
import static com.google.common.net.HttpHeaders.CONTENT_RANGE;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_LENGTH;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.HttpHeaders.IF_MATCH;
import static jakarta.ws.rs.core.HttpHeaders.IF_MODIFIED_SINCE;
import static jakarta.ws.rs.core.HttpHeaders.IF_NONE_MATCH;
import static jakarta.ws.rs.core.HttpHeaders.IF_UNMODIFIED_SINCE;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static org.projectnessie.objectstoragemock.adlsgen2.DataLakeStorageError.dataLakeStorageErrorObj;
import static org.projectnessie.objectstoragemock.s3.S3Constants.RANGE;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Stream;
import org.projectnessie.objectstoragemock.adlsgen2.FilesystemResourceType;
import org.projectnessie.objectstoragemock.adlsgen2.GetPropertiesAction;
import org.projectnessie.objectstoragemock.adlsgen2.ImmutablePath;
import org.projectnessie.objectstoragemock.adlsgen2.ImmutablePathList;
import org.projectnessie.objectstoragemock.adlsgen2.UpdateAction;
import org.projectnessie.objectstoragemock.util.Holder;
import org.projectnessie.objectstoragemock.util.PrefixSpliterator;
import org.projectnessie.objectstoragemock.util.StartAfterSpliterator;

@Path("/adlsgen2/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AdlsGen2Resource {
  @Inject ObjectStorageMock mockServer;

  static final String delimiter = "/";

  // See
  // https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create?view=rest-storageservices-datalakestoragegen2-2019-12-12
  @PUT
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}/{path:.*}")
  @Consumes(MediaType.WILDCARD)
  public Response create(
      @PathParam("filesystem") String filesystem,
      @PathParam("path") String path,
      @QueryParam("continuation") String continuationToken,
      @QueryParam("mode") String mode,
      @QueryParam("timeout") Integer timeoutSeconds,
      @HeaderParam("Cache-Control") String cacheControl,
      @HeaderParam("Content-Encoding") String contentEncoding,
      @HeaderParam("Content-Language") String contentLanguage,
      @HeaderParam("Content-Disposition") String contentDisposition,
      @HeaderParam(CONTENT_TYPE) String contentType,
      @HeaderParam("x-ms-cache-control") String msCacheControl,
      @HeaderParam("x-ms-content-type") String msContentType,
      @HeaderParam("x-ms-content-encoding") String msContentEncoding,
      @HeaderParam("x-ms-content-language") String msContentLanguage,
      @HeaderParam("x-ms-content-disposition") String msContentDisposition,
      @HeaderParam("x-ms-rename-source") String msRenameSource,
      @HeaderParam("x-ms-properties") String msProperties,
      @HeaderParam(IF_MATCH) String ifMatch,
      @HeaderParam(IF_NONE_MATCH) String ifNoneMatch,
      @HeaderParam(IF_MODIFIED_SINCE) String ifModifiedSince,
      @HeaderParam(IF_UNMODIFIED_SINCE) String ifUnmodifiedSince,
      @HeaderParam("x-ms-source-if-match") String msSourceIfMatch,
      @HeaderParam("x-ms-source-if-none-match") String msSourceIfNoneMatch,
      @HeaderParam("x-ms-source-if-modified-since") String msSourceIfModifiedSince,
      @HeaderParam("x-ms-source-if-unmodified-since") String msSourceIfUUnmodifiedSince,
      @HeaderParam("x-ms-encryption-key") String msEncryptionKey,
      @HeaderParam("x-ms-encryption-key-sha256") String msEncryptionKeySha256,
      @HeaderParam("x-ms-encryption-algorithm") String msEncryptionAlgorithm,
      @HeaderParam("x-ms-encryption-context") String msEncryptionContext,
      @HeaderParam("x-ms-date") String msDate) {

    return withFilesystem(
        filesystem,
        b -> {
          b.updater().update(path, Bucket.UpdaterMode.CREATE_NEW).commit();
          return Response.status(Status.CREATED)
              //              .header("Date", "")
              //              .header("ETag", "")
              //              .header("Last-Modified", "")
              //              .header("x-ms-request-id", "")
              //              .header("x-ms-version", "")
              //              .header("x-ms-continuation", "")
              //              .header(CONTENT_LENGTH, "")
              //              .header("x-ms-request-server-encrypted", "")
              //              .header("x-ms-encryption-key-sha256", "")
              //              .header("x-ms-encryption-scope", "")
              .build();
        });
  }

  @PATCH
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}/{path:.*}")
  @Consumes(MediaType.WILDCARD)
  public Response update(
      @PathParam("filesystem") String filesystem,
      @PathParam("path") String path,
      @QueryParam("action") UpdateAction action,
      @QueryParam("position") Long position,
      @QueryParam("retainUncommittedData") String retainUncommittedData,
      @QueryParam("close") String close,
      @QueryParam("mode") String mode,
      @QueryParam("maxRecords") Integer maxRecords,
      @QueryParam("forceFlag") @DefaultValue("false") boolean forceFlag,
      @QueryParam("continuation") String continuationToken,
      @QueryParam("flush") @DefaultValue("false") boolean flush,
      @QueryParam("timeout") Integer timeoutSeconds,
      @HeaderParam(CONTENT_LENGTH) Long contentLength,
      @HeaderParam(CONTENT_MD5) String contentMD5,
      @HeaderParam(CONTENT_TYPE) String contentType,
      @HeaderParam("x-ms-cache-control") String msCacheControl,
      @HeaderParam("x-ms-content-type") String msContentType,
      @HeaderParam("x-ms-content-encoding") String msContentEncoding,
      @HeaderParam("x-ms-content-language") String msContentLanguage,
      @HeaderParam("x-ms-content-disposition") String msContentDisposition,
      @HeaderParam("x-ms-content-md5") String msContentMD5,
      @HeaderParam("x-ms-properties") String msProperties,
      @HeaderParam("x-ms-owner") String msOwner,
      @HeaderParam("x-ms-group") String msGroup,
      @HeaderParam("x-ms-permissions") String msPermissions,
      @HeaderParam("x-ms-acl") String msAcl,
      @HeaderParam(IF_MATCH) String ifMatch,
      @HeaderParam(IF_NONE_MATCH) String ifNoneMatch,
      @HeaderParam(IF_MODIFIED_SINCE) String ifModifiedSince,
      @HeaderParam(IF_UNMODIFIED_SINCE) String ifUnmodifiedSince,
      @HeaderParam("x-ms-encryption-key") String msEncryptionKey,
      @HeaderParam("x-ms-encryption-key-sha256") String msEncryptionKeySha256,
      @HeaderParam("x-ms-encryption-algorithm") String msEncryptionAlgorithm,
      @HeaderParam("x-ms-encryption-context") String msEncryptionContext,
      @HeaderParam("x-ms-date") String msDate,
      InputStream input) {

    return withFilesystem(
        filesystem,
        b -> {
          if (!action.appendOrFlush()) {
            return notImplemented();
          }
          Bucket.ObjectUpdater updater = b.updater().update(path, Bucket.UpdaterMode.UPDATE);
          if (updater == null) {
            return keyNotFound();
          }
          MockObject o;
          if (action == UpdateAction.append) {
            updater.append(0L, input);
          }
          boolean doFlush = action == UpdateAction.flush || flush;
          if (doFlush) {
            updater
                .flush()
                .setContentType(msContentType != null ? msContentType : "application/octet-stream");
          }
          updater.commit();
          return Response.status(doFlush ? Status.OK : Status.ACCEPTED).build();
          //              .header("Date", "")
          //              .header("ETag", "")
          //              .header("Last-Modified", "")
          //              .header("Accept-Ranges", "")
          //              .header("Cache-Control", "")
          //              .header("Content-Disposition", "")
          //              .header("Content-Encoding", "")
          //              .header("Content-Language", "")
          //              .header("Content-Length: integer
          //              .header("Content-Range", "")
          //              .header(CONTENT_TYPE, "")
          //              .header(CONTENT_MD5, "")
          //              .header("x-ms-properties", "")
          //              .header("x-ms-continuation", "")
          //              .header("x-ms-request-id", "")
          //              .header("x-ms-version", "")
          //              .header("x-ms-request-server-encrypted", "")
          //              .header("x-ms-encryption-key-sha256", "")
          //              .header("x-ms-encryption-scope", "")
          //              .header("x-ms-lease-renewed", "")
        });
  }

  @GET
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}/{path:.*}")
  @Produces(MediaType.WILDCARD)
  public Response read(
      @PathParam("filesystem") String filesystem,
      @PathParam("path") String path,
      @QueryParam("timeout") Integer timeoutSeconds,
      @HeaderParam(RANGE) Range range,
      @HeaderParam("x-ms-range-get-content-md5 ") String msRangeGetContentMd5,
      @HeaderParam(IF_MATCH) String ifMatch,
      @HeaderParam(IF_NONE_MATCH) String ifNoneMatch,
      @HeaderParam(IF_MODIFIED_SINCE) String ifModifiedSince,
      @HeaderParam(IF_UNMODIFIED_SINCE) String ifUnmodifiedSince,
      @HeaderParam("x-ms-encryption-key") String msEncryptionKey,
      @HeaderParam("x-ms-encryption-key-sha256") String msEncryptionKeySha256,
      @HeaderParam("x-ms-encryption-algorithm") String msEncryptionAlgorithm,
      @HeaderParam("x-ms-date") String msDate) {

    return withFilesystem(
        filesystem,
        b -> {
          MockObject obj = b.object().retrieve(path);
          if (obj == null) {
            return keyNotFound();
          }

          String contentType;
          switch (obj.contentType()) {
            case "text/plain":
            case "application/json":
              contentType = obj.contentType();
              break;
            default:
              contentType = "application/octet-stream";
              break;
          }

          StreamingOutput stream = output -> obj.writer().write(range, output);

          long start = range != null ? range.start() : 0L;
          long end =
              range != null ? Math.min(range.end(), obj.contentLength()) : obj.contentLength();

          return Response.ok(stream)
              .tag(obj.etag())
              .type(contentType)
              .header(CONTENT_LENGTH, obj.contentLength())
              .header(CONTENT_RANGE, "bytes " + start + "-" + end + "/" + obj.contentLength())
              .lastModified(new Date(obj.lastModified()))
              .build();

          //    Accept-Ranges: string
          //    Cache-Control: string
          //    Content-Disposition: string
          //    Content-Encoding: string
          //    Content-Language: string
          //    Content-Length: integer
          //    Content-Range: string
          //    Content-MD5: string
          //    Date: string
          //    x-ms-request-id: string
          //    x-ms-version: string
          //    x-ms-resource-type: string
          //    x-ms-properties: string
          //    x-ms-lease-duration: string
          //    x-ms-lease-state: string
          //    x-ms-lease-status: string
          //    x-ms-server-encrypted: true/false: boolean
          //    x-ms-encryption-key-sha256: string
          //    x-ms-encryption-context: string
          //    x-ms-encryption-scope: string
        });
  }

  @HEAD
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}/{path:.*}")
  @Produces(MediaType.WILDCARD)
  public Response getProperties(
      @PathParam("filesystem") String filesystem,
      @PathParam("path") String path,
      @QueryParam("action") GetPropertiesAction action,
      @QueryParam("fsAction") String fsAction,
      @QueryParam("timeout") Integer timeoutSeconds,
      @QueryParam("upn") @DefaultValue("false") boolean upn,
      @HeaderParam(IF_MATCH) String ifMatch,
      @HeaderParam(IF_NONE_MATCH) String ifNoneMatch,
      @HeaderParam(IF_MODIFIED_SINCE) String ifModifiedSince,
      @HeaderParam(IF_UNMODIFIED_SINCE) String ifUnmodifiedSince,
      @HeaderParam("x-ms-encryption-key") String msEncryptionKey,
      @HeaderParam("x-ms-encryption-key-sha256") String msEncryptionKeySha256,
      @HeaderParam("x-ms-encryption-algorithm") String msEncryptionAlgorithm,
      @HeaderParam("x-ms-date") String msDate) {

    return withFilesystem(
        filesystem,
        b -> {
          MockObject obj = b.object().retrieve(path);
          if (obj == null) {
            return keyNotFound();
          }

          return Response.ok()
              .tag(obj.etag())
              .type(obj.contentType())
              .header(CONTENT_LENGTH, obj.contentLength())
              .lastModified(new Date(obj.lastModified()))
              .build();

          //    Accept-Ranges: string
          //    Cache-Control: string
          //    Content-Disposition: string
          //    Content-Encoding: string
          //    Content-Language: string
          //    Content-Length: integer
          //    Content-Range: string
          //    Content-MD5: string
          //    Date: string
          //    x-ms-request-id: string
          //    x-ms-version: string
          //    x-ms-resource-type: string
          //    x-ms-properties: string
          //    x-ms-lease-duration: string
          //    x-ms-lease-state: string
          //    x-ms-lease-status: string
          //    x-ms-server-encrypted: true/false: boolean
          //    x-ms-encryption-key-sha256: string
          //    x-ms-encryption-context: string
          //    x-ms-encryption-scope: string
        });
  }

  // See
  // https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list?view=rest-storageservices-datalakestoragegen2-2019-12-12
  @DELETE
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}/{path:.*}")
  @Consumes(MediaType.WILDCARD)
  public Response delete(
      @PathParam("filesystem") String filesystem,
      @PathParam("path") String path,
      @QueryParam("continuation") String continuationToken,
      @QueryParam("paginated") @DefaultValue("false") boolean paginated,
      @QueryParam("recursive") @DefaultValue("false") boolean recursive,
      @QueryParam("timeout") Integer timeoutSeconds,
      @HeaderParam(IF_MATCH) String ifMatch,
      @HeaderParam(IF_NONE_MATCH) String ifNoneMatch,
      @HeaderParam(IF_MODIFIED_SINCE) String ifModifiedSince,
      @HeaderParam(IF_UNMODIFIED_SINCE) String ifUnmodifiedSince) {
    // No clue why there are pagination parameters, although there's no response

    return withFilesystem(
        filesystem,
        b -> {
          if (recursive) {
            try (Stream<Bucket.ListElement> listStream = b.lister().list(path, continuationToken)) {
              splitForDirectory(path, continuationToken, listStream)
                  .forEachRemaining(e -> b.deleter().delete(e.key()));
            }
          } else {
            MockObject o = b.object().retrieve(path);
            if (o == null) {
              return keyNotFound();
            }

            if (!b.deleter().delete(path)) {
              return keyNotFound();
            }
          }
          return Response.ok().build();
        });
  }

  // See
  // https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list?view=rest-storageservices-datalakestoragegen2-2019-12-12
  @GET
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}")
  public Response list(
      @PathParam("filesystem") String filesystem,
      @QueryParam("directory") String directory,
      @QueryParam("recursive") @DefaultValue("false") boolean recursive,
      @QueryParam("continuation") String continuationToken,
      @QueryParam("maxResults") Integer maxResults,
      @QueryParam("upn") @DefaultValue("false") boolean upn,
      @QueryParam("resoure") FilesystemResourceType resoure,
      @QueryParam("timeout") Integer timeoutSeconds) {

    // TODO handle 'recursive' - it's special, like everything from MS

    return withFilesystem(
        filesystem,
        b -> {
          try (Stream<Bucket.ListElement> listStream =
              b.lister().list(directory, continuationToken)) {
            ImmutablePathList.Builder result = ImmutablePathList.builder();

            int maxKeys = maxResults != null ? maxResults : Integer.MAX_VALUE;

            String nextContinuationToken = null;
            int keyCount = 0;
            String lastKey = null;

            Spliterator<Bucket.ListElement> split =
                splitForDirectory(directory, continuationToken, listStream);

            Holder<Bucket.ListElement> current = new Holder<>();
            while (split.tryAdvance(current::set)) {
              if (keyCount == maxKeys) {
                nextContinuationToken = lastKey;
                break;
              }

              String key = current.get().key();

              MockObject obj = current.get().object();
              result.addPaths(
                  ImmutablePath.builder()
                      .name(key)
                      .etag(obj.etag())
                      .contentLength(obj.contentLength())
                      .lastModified(
                          RFC_1123_DATE_TIME.format(
                              ZonedDateTime.ofInstant(
                                  Instant.ofEpochMilli(obj.lastModified()), ZoneId.of("UTC"))))
                      .directory(false)
                      .build());
              keyCount++;
              lastKey = key;
            }

            Response.ResponseBuilder response = Response.ok(result.build());
            if (nextContinuationToken != null) {
              response.header("x-ms-continuation", nextContinuationToken);
            }
            return response.build();
          }
        });
  }

  private static Spliterator<Bucket.ListElement> splitForDirectory(
      String directory, String offset, Stream<Bucket.ListElement> listStream) {

    Spliterator<Bucket.ListElement> split = listStream.spliterator();

    if (offset != null) {
      split = new StartAfterSpliterator<>(split, e -> e.key().compareTo(offset) >= 0);
    }

    if (directory == null || directory.isEmpty()) {
      return split;
    }

    if (!directory.endsWith(delimiter)) {
      directory += delimiter;
    }

    String directoryPrefix = directory;
    return new PrefixSpliterator<>(split, e -> e.key().startsWith(directoryPrefix));
  }

  private static Response preconditionFailed() {
    return dataLakeStorageError(
        Status.PRECONDITION_FAILED,
        "ConditionNotMet",
        "The condition specified using HTTP conditional header(s) is not met.");
  }

  private static Response notModified(String etag) {
    // Hint: HTTP/304 MUST NOT contain a message body (as per HTTP RFCs)
    return Response.notModified(etag).build();
  }

  private static Response noContent() {
    return Response.status(Status.NO_CONTENT).build();
  }

  private static Response bucketNotFound() {
    return dataLakeStorageError(
        Status.NOT_FOUND, "FilesystemNotFound", "The specified filesystem does not exist.");
  }

  private static Response keyNotFound() {
    return dataLakeStorageError(
        Status.NOT_FOUND, "PathNotFound", "The specified path does not exist.");
  }

  private static Response dataLakeStorageError(Status status, String code, String message) {
    return Response.status(status)
        .header("x-ms-error-code", code)
        .type(MediaType.APPLICATION_JSON)
        .entity(dataLakeStorageErrorObj(code, message).error())
        .build();
  }

  private static Response notImplemented() {
    return Response.status(Status.NOT_IMPLEMENTED).build();
  }

  private Response withFilesystem(String filesystem, Function<Bucket, Response> worker) {
    Bucket bucket = mockServer.buckets().get(filesystem);
    if (bucket == null) {
      return bucketNotFound();
    }
    return worker.apply(bucket);
  }

  private Response withFilesystemObject(
      String filesystem, String objectName, Function<MockObject, Response> worker) {
    return withFilesystem(
        filesystem,
        bucket -> {
          MockObject o = bucket.object().retrieve(objectName);
          if (o == null) {
            return keyNotFound();
          }
          return worker.apply(o);
        });
  }
}
