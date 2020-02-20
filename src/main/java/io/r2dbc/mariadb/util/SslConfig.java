/*
 * Copyright 2020 MariaDB Ab.
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

package io.r2dbc.mariadb.util;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.r2dbc.mariadb.SslMode;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SslConfig {

  public static final SslConfig DISABLE_INSTANCE = new SslConfig(SslMode.DISABLED);

  private SslMode sslMode;
  private String serverSslCert;
  private String clientSslCert;
  private String clientSslKey;
  private CharSequence clientSslPassword;
  private List<String> tlsProtocol;

  public SslConfig(
      SslMode sslMode,
      String serverSslCert,
      String clientSslCert,
      String clientSslKey,
      CharSequence clientSslPassword,
      List<String> tlsProtocol) {
    this.sslMode = sslMode;
    this.serverSslCert = serverSslCert;
    this.clientSslCert = clientSslCert;
    this.tlsProtocol = tlsProtocol;
    this.clientSslCert = clientSslCert;
    this.clientSslKey = clientSslKey;
    this.clientSslPassword = clientSslPassword;
  }

  public SslConfig(SslMode sslMode) {
    this.sslMode = sslMode;
  }

  public SslMode getSslMode() {
    return sslMode;
  }

  public String getServerSslCert() {
    return serverSslCert;
  }

  public String getClientSslCert() {
    return clientSslCert;
  }

  public List<String> getTlsProtocol() {
    return tlsProtocol;
  }

  public SslContext getSslContext() throws R2dbcTransientResourceException, SSLException {
    final SslContextBuilder sslCtxBuilder = SslContextBuilder.forClient();

    if (sslMode == SslMode.ENABLE_TRUST) {
      sslCtxBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
    } else {

      if (serverSslCert != null) {
        try {
          InputStream inStream = loadCert(serverSslCert);
          sslCtxBuilder.trustManager(inStream);
        } catch (FileNotFoundException fileNotFoundEx) {
          throw new R2dbcTransientResourceException(
              "Failed to find serverSslCert file. serverSslCert=" + serverSslCert,
              "08000",
              fileNotFoundEx);
        }
      }

      if (clientSslCert != null && clientSslKey != null) {
        InputStream certificatesStream;
        try {
          certificatesStream = loadCert(clientSslCert);
        } catch (FileNotFoundException fileNotFoundEx) {
          throw new R2dbcTransientResourceException(
              "Failed to find clientSslCert file. clientSslCert=" + clientSslCert,
              "08000",
              fileNotFoundEx);
        }

        try {
          InputStream privateKeyStream = new FileInputStream(clientSslKey);
          sslCtxBuilder.keyManager(
              certificatesStream,
              privateKeyStream,
              clientSslPassword == null ? null : clientSslPassword.toString());
        } catch (FileNotFoundException fileNotFoundEx) {
          throw new R2dbcTransientResourceException(
              "Failed to find clientSslKey file. clientSslKey=" + clientSslKey,
              "08000",
              fileNotFoundEx);
        }
      }
    }

    if (tlsProtocol != null) {
      sslCtxBuilder.protocols(tlsProtocol.toArray(new String[tlsProtocol.size()]));
    }
    return sslCtxBuilder.build();
  }

  private InputStream loadCert(String path) throws FileNotFoundException {
    InputStream inStream = null;
    // generate a keyStore from the provided cert
    if (path.startsWith("-----BEGIN CERTIFICATE-----")) {
      inStream = new ByteArrayInputStream(path.getBytes());
    } else if (path.startsWith("classpath:")) {
      // Load it from a classpath relative file
      String classpathFile = path.substring("classpath:".length());
      inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(classpathFile);
    } else {
      inStream = new FileInputStream(path);
    }
    return inStream;
  }

  public GenericFutureListener<Future<? super io.netty.channel.Channel>> getHostNameVerifier(
      CompletableFuture<Void> result, String host, long threadId, SSLEngine engine) {
    return future -> {
      if (!future.isSuccess()) {
        result.completeExceptionally(future.cause());
        return;
      }
      if (sslMode == SslMode.ENABLE) {
        try {
          DefaultHostnameVerifier hostnameVerifier = new DefaultHostnameVerifier();
          SSLSession session = engine.getSession();
          if (!hostnameVerifier.verify(host, session, threadId)) {

            // Use proprietary verify method in order to have an exception with a better description
            // of error.
            Certificate[] certs = session.getPeerCertificates();
            X509Certificate cert = (X509Certificate) certs[0];
            hostnameVerifier.verify(host, cert, threadId);
          }
        } catch (SSLException ex) {
          result.completeExceptionally(
              new R2dbcNonTransientResourceException(
                  "SSL hostname verification failed : " + ex.getMessage(), "08006"));
          return;
        }
      }
      result.complete(null);
    };
  }

  @Override
  public String toString() {
    return "SslConfig{"
        + "sslMode="
        + sslMode
        + ", serverSslCert='"
        + serverSslCert
        + '\''
        + ", clientSslCert='"
        + clientSslCert
        + '\''
        + ", tlsProtocol="
        + tlsProtocol
        + '}';
  }
}
