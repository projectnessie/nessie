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
package org.projectnessie.nessie.combined;

import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

public final class EmptyHttpHeaders implements HttpHeaders {

  private static final EmptyHttpHeaders INSTANCE = new EmptyHttpHeaders();

  private EmptyHttpHeaders() {}

  public static HttpHeaders emptyHttpHeaders() {
    return INSTANCE;
  }

  @Override
  public List<String> getRequestHeader(String name) {
    return null;
  }

  @Override
  public String getHeaderString(String name) {
    return null;
  }

  @Override
  public boolean containsHeaderString(
      String name, String valueSeparatorRegex, Predicate<String> valuePredicate) {
    return false;
  }

  @Override
  public MultivaluedMap<String, String> getRequestHeaders() {
    return new MultivaluedHashMap<>();
  }

  @Override
  public List<MediaType> getAcceptableMediaTypes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Locale> getAcceptableLanguages() {
    throw new UnsupportedOperationException();
  }

  @Override
  public MediaType getMediaType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Locale getLanguage() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Cookie> getCookies() {
    return Map.of();
  }

  @Override
  public Date getDate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLength() {
    throw new UnsupportedOperationException();
  }
}
