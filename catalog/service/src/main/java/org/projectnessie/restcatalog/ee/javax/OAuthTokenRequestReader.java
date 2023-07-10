/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.restcatalog.ee.javax;

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import org.apache.commons.io.IOUtils;
import org.projectnessie.restcatalog.api.model.ImmutableOAuthTokenRequest;
import org.projectnessie.restcatalog.api.model.OAuthTokenRequest;

@Provider
public class OAuthTokenRequestReader implements MessageBodyReader<OAuthTokenRequest> {

  @Override
  public boolean isReadable(
      Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return type == OAuthTokenRequest.class
        && mediaType.isCompatible(MediaType.APPLICATION_FORM_URLENCODED_TYPE);
  }

  @Override
  public OAuthTokenRequest readFrom(
      Class<OAuthTokenRequest> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream)
      throws WebApplicationException {
    try {
      String contentLength = httpHeaders.getFirst(HttpHeaders.CONTENT_LENGTH);
      byte[] bb =
          contentLength != null
              ? IOUtils.toByteArray(entityStream, Integer.parseInt(contentLength))
              : IOUtils.toByteArray(entityStream);
      return ImmutableOAuthTokenRequest.builder().headers(httpHeaders).body(bb).build();
    } catch (Exception e) {
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    }
  }
}
