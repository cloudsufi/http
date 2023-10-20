/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.http.sink.batch;

import com.google.auth.oauth2.AccessToken;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.http.common.http.HttpClient;
import io.cdap.plugin.http.common.http.OAuthUtil;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * RecordWriter for HTTP.
 */
public class HTTPRecordWriter extends RecordWriter<StructuredRecord, StructuredRecord> {
  private final HTTPSinkConfig config;
  private final MessageBuffer messageBuffer;
  private String contentType;
  private AccessToken accessToken;

  HTTPRecordWriter(HTTPSinkConfig config, Schema inputSchema) {
    this.config = config;
    this.accessToken = null;
    this.messageBuffer = new MessageBuffer(
            config.getMessageFormat(), config.getJsonBatchKey(), config.shouldWriteJsonAsArray(),
            config.getDelimiterForMessages(), config.getCharset(), config.getBody(), inputSchema
    );
  }

  @Override
  public void write(StructuredRecord input, StructuredRecord unused) throws IOException {
    if (config.getMethod().equals("POST") || config.getMethod().equals("PUT")) {
      messageBuffer.add(input);
    }
    if (config.getBatchSize() == messageBuffer.size()) {
      flushMessageBuffer();
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException {
    // Process remaining messages after batch executions.
    flushMessageBuffer();
  }

  private void executeHTTPService() throws IOException {
    int responseCode;
    int retries = 0;
    IOException exception = null;
    CloseableHttpClient httpClient = HttpClients.createDefault();
    do {
      exception = null;
      CloseableHttpResponse response = null;

      Map<String, String> headers = config.getRequestHeadersMap();

      if (accessToken == null || OAuthUtil.tokenExpired(accessToken)) {
        accessToken = OAuthUtil.getAccessToken(config);
      }

      if (accessToken != null) {
        Header authorizationHeader = new BasicHeader("Authorization",
          String.format("Bearer %s", accessToken.getTokenValue()));
        headers.putAll(config.getHeadersMap(String.valueOf(authorizationHeader)));
      }

      headers.put("Request-Method", config.getMethod().toUpperCase());
      headers.put("Connect-Timeout", String.valueOf(config.getConnectTimeout()));
      headers.put("Read-Timeout", String.valueOf(config.getReadTimeout()));
      headers.put("Instance-Follow-Redirects", String.valueOf(config.getFollowRedirects()));
      headers.put("charset", config.getCharset());


      try {
        URL url = new URL(config.getUrl());
        HttpEntityEnclosingRequestBase request = new HttpClient.HttpRequest(URI.create(String.valueOf(url)),
          config.getMethod());

        if (!Strings.isNullOrEmpty(config.getProxyUrl())) {
          URL proxyURL = new URL(config.getProxyUrl());
          String proxyHost = proxyURL.getHost();
          int proxyPort = proxyURL.getPort();
          String proxyUser = config.getProxyUsername();
          String proxyPassword = config.getProxyPassword();

          CredentialsProvider credsProvider = new BasicCredentialsProvider();
          credsProvider.setCredentials(
            new AuthScope(proxyHost, proxyPort),
            new UsernamePasswordCredentials(proxyUser, proxyPassword));

          HttpHost proxy = new HttpHost(proxyHost, proxyPort);
          httpClient = HttpClients.custom()
            .setDefaultCredentialsProvider(credsProvider)
            .setProxy(proxy)
            .build();
        }

        if (url.getProtocol().equalsIgnoreCase("https")) {
          // Disable SSLv3
          System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
          if (config.getDisableSSLValidation()) {
            disableSSLValidation();
          }
        }

        if (config.getMethod().equals("POST") || config.getMethod().equals("PUT")) {
          if (!headers.containsKey("Content-Type")) {
            headers.put("Content-Type", contentType);
          }
        }

        if (!messageBuffer.isEmpty()) {
          String requestBodyString = messageBuffer.getMessage();
          if (requestBodyString != null) {
            StringEntity requestBody = new StringEntity(requestBodyString, Charsets.UTF_8.toString());
            request.setEntity(requestBody);
          }
        }

        for (Map.Entry<String, String> propertyEntry : headers.entrySet()) {
          request.addHeader(propertyEntry.getKey(), propertyEntry.getValue());
        }

        response = httpClient.execute(request);

        responseCode = response.getStatusLine().getStatusCode();

        messageBuffer.clear();
        if (config.getFailOnNon200Response() && !(responseCode >= 200 && responseCode < 300)) {
          exception = new IOException("Received error response. Response code: " + responseCode);
        }
        break;
      } catch (IOException e) {
        exception = new IOException("An error occurred while executing the HTTP service: " + e.getMessage(), e);
      } finally {
        if (response != null) {
          response.close();
        }
      }
      retries++;
    } while (retries < config.getNumRetries());
    if (exception != null) {
      throw exception;
    }
  }

  private void disableSSLValidation() {
    TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return null;
      }

      public void checkClientTrusted(X509Certificate[] certs, String authType) {
      }

      public void checkServerTrusted(X509Certificate[] certs, String authType) {
      }
    }
    };
    SSLContext sslContext = null;
    try {
      sslContext = SSLContext.getInstance("SSL");
      sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
    } catch (KeyManagementException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Error while installing the trust manager: " + e.getMessage(), e);
    }
    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
    HostnameVerifier allHostsValid = new HostnameVerifier() {
      public boolean verify(String hostname, SSLSession session) {
        return true;
      }
    };
    HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
  }

  private void flushMessageBuffer() throws IOException {
    if (messageBuffer.isEmpty()) {
      return;
    }
    contentType = messageBuffer.getContentType();
    executeHTTPService();
    messageBuffer.clear();
  }

}
