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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.serverless.proxy.internal.LambdaContainerHandler;
import com.amazonaws.serverless.proxy.internal.testutils.AwsProxyRequestBuilder;
import com.amazonaws.serverless.proxy.internal.testutils.MockLambdaContext;
import com.amazonaws.serverless.proxy.model.AwsProxyResponse;
import com.amazonaws.services.lambda.runtime.Context;
import com.dremio.nessie.auth.AuthResponse;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.serverless.NessieLambda;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


public class StreamLambdaHandlerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static NessieLambda handler;
  private static Context lambdaContext;

  @BeforeAll
  public static void setUp() {
    handler = new NessieLambda();
    lambdaContext = new MockLambdaContext();
  }

  @Test
  public void ping_streamRequest_respondsWithHello() throws JsonProcessingException {
    InputStream requestStream = new AwsProxyRequestBuilder("/login", HttpMethod.POST)
        .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
        .form("username", "admin_user")
        .form("password", "test123")
        .buildStream();
    ByteArrayOutputStream responseStream = new ByteArrayOutputStream();

    handle(requestStream, responseStream);

    AwsProxyResponse response = readResponse(responseStream);
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatusCode());

    assertFalse(response.isBase64Encoded());
    AuthResponse auth = MAPPER.readValue(response.getBody(), AuthResponse.class);
    requestStream = new AwsProxyRequestBuilder("/objects", HttpMethod.GET)
      .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
      .header(HttpHeaders.AUTHORIZATION, response
        .getMultiValueHeaders()
        .getFirst(HttpHeaders.AUTHORIZATION))
      .buildStream();
    responseStream = new ByteArrayOutputStream();

    handle(requestStream, responseStream);

    response = readResponse(responseStream);
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatusCode());
    Branch[] branches = MAPPER.readValue(response.getBody(), Branch[].class);
    assertEquals(1, branches.length);
    assertEquals("master", branches[0].getName());

    assertTrue(response.getMultiValueHeaders().containsKey(HttpHeaders.CONTENT_TYPE));
    assertTrue(response.getMultiValueHeaders()
                       .getFirst(HttpHeaders.CONTENT_TYPE)
                       .startsWith(MediaType.APPLICATION_JSON));
  }

  @Test
  public void invalidResource_streamRequest_responds404() {
    InputStream requestStream = new AwsProxyRequestBuilder("/pong", HttpMethod.GET)
        .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
        .buildStream();
    ByteArrayOutputStream responseStream = new ByteArrayOutputStream();

    handle(requestStream, responseStream);

    AwsProxyResponse response = readResponse(responseStream);
    assertNotNull(response);
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatusCode());
  }

  private void handle(InputStream is, ByteArrayOutputStream os) {
    try {
      handler.handleRequest(is, os, lambdaContext);
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private AwsProxyResponse readResponse(ByteArrayOutputStream responseStream) {
    try {
      return LambdaContainerHandler.getObjectMapper()
                                   .readValue(responseStream.toByteArray(), AwsProxyResponse.class);
    } catch (IOException e) {
      e.printStackTrace();
      fail("Error while parsing response: " + e.getMessage());
    }
    return null;
  }
}
