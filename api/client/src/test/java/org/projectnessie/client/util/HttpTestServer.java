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
package org.projectnessie.client.util;

import io.undertow.Undertow;
import io.undertow.Undertow.ListenerBuilder;
import io.undertow.Undertow.ListenerInfo;
import io.undertow.Undertow.ListenerType;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.encoding.EncodingHandler;
import io.undertow.server.handlers.encoding.RequestEncodingHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.InstanceHandle;
import jakarta.servlet.Servlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStore.TrustedCertificateEntry;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/** HTTP test server. */
public class HttpTestServer implements AutoCloseable {
  private final URI uri;
  private final Undertow server;
  private SSLContext sslContext;

  @FunctionalInterface
  public interface RequestHandler {
    void handle(HttpServletRequest request, HttpServletResponse response) throws IOException;
  }

  public HttpTestServer(RequestHandler handler) throws Exception {
    this(handler, false);
  }

  public HttpTestServer(RequestHandler handler, boolean ssl) throws Exception {
    this("/", handler, ssl);
  }

  public HttpTestServer(String context, RequestHandler handler) throws Exception {
    this(context, handler, false);
  }

  /**
   * Constructor.
   *
   * @param context server context
   * @param handler http request handler
   * @param ssl whether to use https instead of http
   */
  public HttpTestServer(String context, RequestHandler handler, boolean ssl) throws Exception {
    DeploymentInfo servletBuilder =
        Servlets.deployment()
            .setClassLoader(HttpTestServer.class.getClassLoader())
            .setContextPath(context)
            .setDeploymentName("test-nessie-client")
            .addServlets(
                Servlets.servlet(
                        "nessie-client",
                        HttpServlet.class,
                        () ->
                            new InstanceHandle<Servlet>() {
                              final HttpServlet servlet =
                                  new HttpServlet() {
                                    @Override
                                    public void service(
                                        HttpServletRequest request, HttpServletResponse response)
                                        throws IOException {
                                      handler.handle(request, response);
                                    }
                                  };

                              @Override
                              public Servlet getInstance() {
                                return servlet;
                              }

                              @Override
                              public void release() {}
                            })
                    .addInitParam("message", "Hello World")
                    .addMapping("/*"));

    DeploymentManager manager = Servlets.defaultContainer().addDeployment(servletBuilder);
    manager.deploy();

    HttpHandler httpHandler = manager.start();

    httpHandler = new EncodingHandler.Builder().build(Collections.emptyMap()).wrap(httpHandler);
    httpHandler =
        new RequestEncodingHandler.Builder().build(Collections.emptyMap()).wrap(httpHandler);

    Undertow.Builder server = Undertow.builder().setHandler(httpHandler);

    ListenerBuilder listener =
        new ListenerBuilder()
            .setPort(0)
            .setHost("localhost")
            .setType(ssl ? ListenerType.HTTPS : ListenerType.HTTP);
    if (ssl) {
      prepareSslContext();
      listener.setSslContext(sslContext);
    }
    server.setServerOption(UndertowOptions.ENABLE_HTTP2, true);
    server.addListener(listener);

    this.server = server.build();
    this.server.start();

    ListenerInfo li = this.server.getListenerInfo().get(0);
    InetSocketAddress sa = (InetSocketAddress) li.getAddress();
    int port = sa.getPort();

    String scheme = ssl ? "https" : "http";
    this.uri = URI.create(scheme + "://localhost:" + port + "/");
  }

  public SSLContext getSslContext() {
    return sslContext;
  }

  private void prepareSslContext() throws Exception {
    sslContext = SSLContext.getInstance("SSL");
    sslContext.init(SelfSignedSSL.keyManagers, SelfSignedSSL.trustManagers, new SecureRandom());
  }

  public URI getUri() {
    return uri;
  }

  @Override
  public void close() {
    server.stop();
  }

  static final class SelfSignedSSL {

    private static KeyPair generateKeyPair(SecureRandom random) throws Exception {
      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
      keyPairGenerator.initialize(2048, random);
      return keyPairGenerator.generateKeyPair();
    }

    private static X509CertificateHolder generateCertHolder(
        SecureRandom random, ZonedDateTime now, KeyPair keyPair) throws Exception {
      X500NameBuilder nameBuilder =
          new X500NameBuilder(BCStyle.INSTANCE)
              .addRDN(BCStyle.CN, "localhost")
              .addRDN(BCStyle.OU, "Dremio Corp. (auto-generated)")
              .addRDN(BCStyle.O, "Dremio Corp. (auto-generated)")
              .addRDN(BCStyle.L, "Mountain View")
              .addRDN(BCStyle.ST, "California")
              .addRDN(BCStyle.C, "US");

      Date notBefore = Date.from(now.minusDays(1).toInstant());
      Date notAfter = Date.from(now.plusYears(1).toInstant());
      BigInteger serialNumber = new BigInteger(128, random);

      // create a certificate valid for 1 years from now
      // add the main hostname + the alternative hostnames to the SAN extension
      GeneralName[] alternativeSubjectNames = new GeneralName[1];
      alternativeSubjectNames[0] = new GeneralName(GeneralName.dNSName, "localhost");

      X509v3CertificateBuilder certificateBuilder =
          new JcaX509v3CertificateBuilder(
                  nameBuilder.build(),
                  serialNumber,
                  notBefore,
                  notAfter,
                  nameBuilder.build(),
                  keyPair.getPublic())
              .addExtension(
                  Extension.subjectAlternativeName,
                  false,
                  new DERSequence(alternativeSubjectNames));

      // sign the certificate using the private key
      ContentSigner contentSigner;
      try {
        contentSigner =
            new JcaContentSignerBuilder("SHA256WithRSAEncryption").build(keyPair.getPrivate());
      } catch (OperatorCreationException e) {
        throw new GeneralSecurityException(e);
      }
      return certificateBuilder.build(contentSigner);
    }

    private static X509Certificate generateCert(ZonedDateTime now, X509CertificateHolder certHolder)
        throws Exception {

      X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(certHolder);

      // check the validity
      certificate.checkValidity(Date.from(now.toInstant()));

      // make sure the certificate is self-signed
      certificate.verify(certificate.getPublicKey());

      return certificate;
    }

    static final TrustManager[] trustManagers;
    static final KeyManager[] keyManagers;

    static {
      try {
        // Initialise the keystore
        String storeType = KeyStore.getDefaultType();
        KeyStore keyStore = KeyStore.getInstance(storeType);
        KeyStore trustStore = KeyStore.getInstance(storeType);
        keyStore.load(null, null);
        trustStore.load(null, null);
        ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());
        SecureRandom random = new SecureRandom();

        KeyPair keyPair = generateKeyPair(random);
        X509CertificateHolder certHolder = generateCertHolder(random, now, keyPair);
        X509Certificate certificate = generateCert(now, certHolder);

        keyStore.setKeyEntry(
            "AutoGeneratedPrivateKey",
            keyPair.getPrivate(),
            "password".toCharArray(),
            new java.security.cert.Certificate[] {certificate});

        trustStore.setEntry("AutoGeneratedCert", new TrustedCertificateEntry(certificate), null);

        // Set up the key manager factory
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(keyStore, "password".toCharArray());

        // Set up the trust manager factory
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(keyStore);

        keyManagers = kmf.getKeyManagers();
        trustManagers = tmf.getTrustManagers();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
