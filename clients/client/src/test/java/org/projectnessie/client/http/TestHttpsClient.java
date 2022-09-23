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
package org.projectnessie.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import java.math.BigInteger;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStore.TrustedCertificateEntry;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
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
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.projectnessie.client.util.TestHttpUtil;
import org.projectnessie.client.util.TestServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Execution(ExecutionMode.CONCURRENT)
class TestHttpsClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestHttpsClient.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void testHttps() throws Exception {
    TestServer.RequestHandler handler =
        (req, resp) -> {
          Assertions.assertEquals("GET", req.getMethod());
          TestHttpUtil.writeResponseBody(resp, "hello");
        };
    TrustManager[][] trustManager = new TrustManager[1][];
    try (TestServer server =
        new TestServer(
            "/",
            handler,
            s -> {
              try {
                trustManager[0] = ssl(s);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            })) {
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, trustManager[0], new java.security.SecureRandom());
      HttpRequest client =
          HttpClient.builder()
              .setBaseUri(URI.create("https://localhost:" + server.getAddress().getPort()))
              .setObjectMapper(MAPPER)
              .setSslContext(sc)
              .build()
              .newRequest();
      client.get();

      final HttpRequest insecureClient =
          HttpClient.builder()
              .setBaseUri(URI.create("https://localhost:" + server.getAddress().getPort()))
              .setObjectMapper(MAPPER)
              .build()
              .newRequest();
      Assertions.assertThrows(HttpClientException.class, insecureClient::get);
    }
  }

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
                Extension.subjectAlternativeName, false, new DERSequence(alternativeSubjectNames));

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

    String fingerprint =
        BaseEncoding.base16()
            .withSeparator(":", 2)
            .encode(MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
    LOGGER.info("Certificate created (SHA-256 fingerprint: {})", fingerprint);
    return certificate;
  }

  private static TrustManager[] ssl(Server server) throws Exception {
    SSLContext sslContext = SSLContext.getInstance("TLS");

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

    // Set up the HTTPS context and parameters
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

    SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
    sslContextFactory.setNeedClientAuth(false);
    sslContextFactory.setSniRequired(false);
    sslContextFactory.setWantClientAuth(false);
    sslContextFactory.setKeyStore(keyStore);
    sslContextFactory.setKeyStorePassword("password");
    sslContextFactory.setTrustStore(trustStore);

    HttpConfiguration httpsConfiguration = new HttpConfiguration();
    httpsConfiguration.addCustomizer(new SecureRequestCustomizer(false));

    ServerConnector httpsConnector =
        new ServerConnector(
            server,
            new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
            new HttpConnectionFactory(httpsConfiguration));

    Connector connector = server.getConnectors()[0];
    server.removeConnector(connector);
    server.addConnector(httpsConnector);

    return tmf.getTrustManagers();
  }
}
