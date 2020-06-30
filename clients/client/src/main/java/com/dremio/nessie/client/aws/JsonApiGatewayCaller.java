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

package com.dremio.nessie.client.aws;

import com.dremio.nessie.json.ObjectMapperContextResolver;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute;
import software.amazon.awssdk.awscore.AwsExecutionAttribute;
import software.amazon.awssdk.awscore.client.builder.AwsDefaultClientBuilder;
import software.amazon.awssdk.awscore.client.config.AwsClientOption;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.RequestOverrideConfiguration;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.client.handler.ClientExecutionParams;
import software.amazon.awssdk.core.client.handler.SdkSyncClientHandler;
import software.amazon.awssdk.core.http.ExecutionContext;
import software.amazon.awssdk.core.http.HttpResponseHandler;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.core.protocol.MarshallLocation;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.core.traits.LocationTrait;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpFullRequest.Builder;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.protocols.json.AwsJsonProtocol;
import software.amazon.awssdk.protocols.json.AwsJsonProtocolFactory;
import software.amazon.awssdk.protocols.json.BaseAwsJsonProtocolFactory;
import software.amazon.awssdk.protocols.json.JsonOperationMetadata;
import software.amazon.awssdk.utils.IoUtils;
import software.amazon.awssdk.utils.builder.Buildable;

public class JsonApiGatewayCaller extends SdkSyncClientHandler {
  private static final Joiner SLASH = Joiner.on("/");
  private final AwsCredentials credentials;
  private final String endpoint;
  private final SdkClientConfiguration clientConfig;
  private final AwsJsonProtocolFactory protocolFactory;
  private final ObjectMapper mapper = new ObjectMapperContextResolver().getContext(null);

  /**
   * handler for calling AWS API Gateway endpoints.
   */
  protected JsonApiGatewayCaller(SdkClientConfiguration clientConfiguration, String endpoint) {
    super(clientConfiguration);
    this.clientConfig = clientConfiguration;

    this.credentials = DefaultCredentialsProvider.create().resolveCredentials();
    this.endpoint = endpoint;
    this.protocolFactory = init(AwsJsonProtocolFactory.builder()).build();
  }


  private SdkHttpFullRequest prepareRequest(SdkHttpMethod method,
                                            NessieRequest in,
                                            EntityTag ifMatchHeader,
                                            String... path) {
    try {
      URI uri = new URI(SLASH.join(endpoint, SLASH.join(Arrays.stream(path).filter(Objects::nonNull).collect(
        Collectors.toList()))));
      SdkHttpFullRequest.Builder builder = SdkHttpFullRequest.builder()
                                                             .uri(uri)
                                                             .method(method);
      if (in.jsonStr() != null) {
        builder.putHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
               .contentStreamProvider(() -> new ByteArrayInputStream(in.jsonStr().getBytes()));
      }
      if (ifMatchHeader != null) {
        builder.putHeader(HttpHeaders.IF_MATCH, ifMatchHeader.toString());
      }
      return builder.build();
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @Override
  protected <InputT extends SdkRequest, OutputT extends SdkResponse> ExecutionContext
      createExecutionContext(ClientExecutionParams<InputT, OutputT> params,
                             ExecutionAttributes executionAttributes) {
    executionAttributes.putAttribute(AwsSignerExecutionAttribute.SERVICE_CONFIG,
                                     clientConfig.option(SdkClientOption.SERVICE_CONFIGURATION))
                       .putAttribute(AwsSignerExecutionAttribute.AWS_CREDENTIALS, credentials)
                       .putAttribute(AwsSignerExecutionAttribute.SERVICE_SIGNING_NAME,
                                     clientConfig.option(AwsClientOption.SERVICE_SIGNING_NAME))
                       .putAttribute(AwsExecutionAttribute.AWS_REGION,
                                     clientConfig.option(AwsClientOption.AWS_REGION))
                       .putAttribute(AwsExecutionAttribute.ENDPOINT_PREFIX,
                                     clientConfig.option(AwsClientOption.ENDPOINT_PREFIX))
                       .putAttribute(AwsSignerExecutionAttribute.SIGNING_REGION,
                                     clientConfig.option(AwsClientOption.SIGNING_REGION))
                       .putAttribute(SdkExecutionAttribute.CLIENT_TYPE,
                                     clientConfig.option(SdkClientOption.CLIENT_TYPE))
                       .putAttribute(SdkExecutionAttribute.SERVICE_NAME,
                                     clientConfig.option(SdkClientOption.SERVICE_NAME))
                       .putAttribute(SdkExecutionAttribute.ENDPOINT_OVERRIDDEN,
                                     clientConfig.option(SdkClientOption.ENDPOINT_OVERRIDDEN));
    return super.createExecutionContext(params, executionAttributes);
  }

  /**
   * main calling method. Use this to fetch results from API Gateway.
   * @param gatewayOps enum indicating which action to perform.
   * @param object optional serialized json object for message body
   * @return serialized json object response
   */
  public <T> T fetchJson(GatewayOps gatewayOps,
                         String path,
                         String object,
                         EntityTag ifMatchHeader) throws JsonProcessingException {
    NessiePojo pojo = executeCommand(gatewayOps.getName(),
                                     object,
                                     gatewayOps.getMethod(),
                                     gatewayOps.isJson(),
                                     path,
                                     ifMatchHeader);
    if (pojo.jsonStr() != null) {
      return (T) mapper.readValue(pojo.jsonStr(), gatewayOps.getReturnType());
    }
    return null;
  }

  private NessiePojo executeCommand(String command,
                                    String object,
                                    SdkHttpMethod method,
                                    boolean isJson,
                                    String path,
                                    EntityTag ifMatchHeader) {
    JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder()
                                                                   .hasStreamingSuccessResponse(
                                                                     false)
                                                                   .isPayloadJson(isJson)
                                                                   .build();
    HttpResponseHandler<NessiePojo> responseHandler = protocolFactory.createResponseHandler(
        operationMetadata, NessiePojo::builder);

    HttpResponseHandler<AwsServiceException> errorResponseHandler = createErrorResponseHandler(
        protocolFactory,
        operationMetadata);
    return super.execute(new ClientExecutionParams<NessieRequest, NessiePojo>()
                           .withOperationName(command)
                           .withResponseHandler(responseHandler)
                           .withInput((NessieRequest) NessieRequest.builder()
                                                                   .jsonStr(object)
                                                                   .build())
                           .withMarshaller(in -> prepareRequest(method, in, ifMatchHeader, command, path))
                           .withErrorResponseHandler(errorResponseHandler));
  }

  private HttpResponseHandler<AwsServiceException> createErrorResponseHandler(
      BaseAwsJsonProtocolFactory protocolFactory,
      JsonOperationMetadata operationMetadata) {
    return protocolFactory.createErrorResponseHandler(operationMetadata);
  }

  private <T extends BaseAwsJsonProtocolFactory.Builder<T>> T init(T builder) {
    return builder
      .clientConfiguration(clientConfig)
      .defaultServiceExceptionSupplier(AwsServiceException::builder)
      .protocol(AwsJsonProtocol.AWS_JSON)
      .protocolVersion("1.0");
  }

  public static class NessieRequest extends SdkRequest {

    private static final SdkField<String> RETURN_CONSUMED_CAPACITY_FIELD = SdkField.builder(
        MarshallingType.STRING)
        .getter(getter(NessieRequest::jsonStr))
        .setter(setter((builder, s) -> ((BuilderImpl) builder).jsonStr(s)))
        .traits(LocationTrait.builder().location(MarshallLocation.PAYLOAD).locationName("jsonStr").build())
        .build();

    private static final List<SdkField<?>> SDK_FIELDS =
        Collections.unmodifiableList(Lists.newArrayList(RETURN_CONSUMED_CAPACITY_FIELD));
    private final String jsonStr;

    public NessieRequest(String jsonStr) {
      this.jsonStr = jsonStr;
    }

    public String jsonStr() {
      return jsonStr;
    }

    @Override
    public Optional<? extends RequestOverrideConfiguration> overrideConfiguration() {
      return Optional.empty();
    }

    @Override
    public Builder toBuilder() {
      return new NessieRequest.BuilderImpl().jsonStr(jsonStr);
    }

    @Override
    public List<SdkField<?>> sdkFields() {
      return SDK_FIELDS;
    }

    public static BuilderImpl builder() {
      return new NessieRequest.BuilderImpl();
    }

    private static final class BuilderImpl implements Builder {

      private String jsonStr;

      public Builder jsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
        return this;
      }

      @Override
      public RequestOverrideConfiguration overrideConfiguration() {
        return null;
      }

      @Override
      public NessieRequest build() {
        return new NessieRequest(jsonStr);
      }
    }
  }

  public static class NessiePojo extends SdkResponse implements Buildable {

    private static final SdkField<String> RETURN_CONSUMED_CAPACITY_FIELD = SdkField.builder(
        MarshallingType.STRING)
        .getter(getterPojo(NessiePojo::jsonStr))
        .setter(setterPojo(NessiePojo::jsonStr))
        .traits(LocationTrait.builder().location(MarshallLocation.PAYLOAD).locationName("").build())
        .build();

    private static final List<SdkField<?>> SDK_FIELDS = Collections.unmodifiableList(
        Lists.newArrayList(RETURN_CONSUMED_CAPACITY_FIELD));
    private final String jsonStr;
    private final SdkHttpResponse response;

    protected NessiePojo(SdkResponse.Builder builder) {
      super(builder);
      this.jsonStr = ((BuilderImpl) builder).jsonStr;
      response = builder.sdkHttpResponse();
    }

    @Override
    public List<SdkField<?>> sdkFields() {
      return SDK_FIELDS;
    }

    public static SdkPojo builder(SdkHttpFullResponse response) {
      Optional<AbortableInputStream> content = response.content();
      BuilderImpl builder = (BuilderImpl) new BuilderImpl().sdkHttpResponse(response);
      if (content.isPresent()) {
        try {
          builder.jsonStr(IoUtils.toUtf8String(content.get()));
        } catch (IOException e) {
          //pass
        }
      }
      return new NessiePojo(builder);
    }

    @Override
    public BuilderImpl toBuilder() {
      return new BuilderImpl().jsonStr(jsonStr);
    }

    public String jsonStr() {
      return jsonStr;
    }

    public void jsonStr(String jsonStr) {

    }

    @Override
    public Object build() {
      return this;
    }

    public static class BuilderImpl extends SdkResponse.BuilderImpl {

      private String jsonStr;

      public BuilderImpl jsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
        return this;
      }

      @Override
      public SdkResponse build() {
        return new NessiePojo(this);
      }
    }
  }

  public static class Builder extends AwsDefaultClientBuilder<Builder, JsonApiGatewayCaller> {

    private String endpoint;

    @Override
    protected JsonApiGatewayCaller buildClient() {
      try {
        return new JsonApiGatewayCaller(super.syncClientConfiguration(), endpoint);
      } catch (NullPointerException e) {
        throw new RuntimeException(
          "Could not construct ApiGateway client with endpoint " + endpoint, e);
      }
    }

    public Builder endpoint(String endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    @Override
    protected String serviceEndpointPrefix() {
      return endpoint;
    }

    @Override
    protected String signingName() {
      return "execute-api";
    }

    @Override
    protected String serviceName() {
      return "execute-api";
    }

    @Override
    protected final SdkClientConfiguration mergeServiceDefaults(SdkClientConfiguration config) {
      return config.merge(c -> c.option(SdkAdvancedClientOption.SIGNER, defaultSigner()).option(
        SdkClientOption.CRC32_FROM_COMPRESSED_DATA_ENABLED, true));
    }

    private Signer defaultSigner() {
      return Aws4Signer.create();
    }
  }

  private static <T> Function<Object, T> getter(Function<NessieRequest, T> g) {
    return obj -> g.apply((NessieRequest) obj);
  }

  private static <T> BiConsumer<Object, T> setter(BiConsumer<SdkRequest.Builder, T> s) {
    return (obj, val) -> s.accept((SdkRequest.Builder) obj, val);
  }

  private static <T> Function<Object, T> getterPojo(Function<NessiePojo, T> g) {
    return obj -> g.apply((NessiePojo) obj);
  }

  private static <T> BiConsumer<Object, T> setterPojo(BiConsumer<NessiePojo, T> s) {
    return (obj, val) -> s.accept((NessiePojo) obj, val);
  }

}
