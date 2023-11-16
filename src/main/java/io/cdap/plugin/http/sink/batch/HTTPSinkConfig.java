/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;


import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.http.common.BaseHttpConfig;
import io.cdap.plugin.http.source.common.EnumWithValue;
import io.cdap.plugin.http.source.common.RetryPolicy;
import io.cdap.plugin.http.source.common.error.ErrorHandling;
import io.cdap.plugin.http.source.common.error.HttpErrorHandlerEntity;
import io.cdap.plugin.http.source.common.error.RetryableErrorHandling;
import io.cdap.plugin.http.source.common.http.MessageFormatType;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.ws.rs.HttpMethod;

/**
 * Config class for {@link HTTPSink}.
 */
public class HTTPSinkConfig extends BaseHttpConfig {
  public static final String URL = "url";
  public static final String METHOD = "method";
  public static final String BATCH_SIZE = "batchSize";
  public static final String WRITE_JSON_AS_ARRAY = "writeJsonAsArray";
  public static final String JSON_BATCH_KEY = "jsonBatchKey";
  public static final String DELIMETER_FOR_MESSAGE = "delimiterForMessages";
  public static final String MESSAGE_FORMAT = "messageFormat";
  public static final String BODY = "body";
  public static final String REQUEST_HEADERS = "requestHeaders";
  public static final String CHARSET = "charset";
  public static final String FOLLOW_REDIRECTS = "followRedirects";
  public static final String DISABLE_SSL_VALIDATION = "disableSSLValidation";
  public static final String PROPERTY_HTTP_ERROR_HANDLING = "httpErrorsHandling";
  public static final String PROPERTY_ERROR_HANDLING = "errorHandling";
  public static final String PROPERTY_RETRY_POLICY = "retryPolicy";
  public static final String PROPERTY_LINEAR_RETRY_INTERVAL = "linearRetryInterval";
  public static final String PROPERTY_MAX_RETRY_DURATION = "maxRetryDuration";
  public static final String CONNECTION_TIMEOUT = "connectTimeout";
  public static final String READ_TIMEOUT = "readTimeout";
  private static final String KV_DELIMITER = ":";
  private static final String DELIMITER = "\n";
  private static final String REGEX_HASHED_VAR = "#(\\w+)";
  private static final String PLACEHOLDER_FORMAT = "#%s";
  private static final Set<String> METHODS = ImmutableSet.of(HttpMethod.GET, HttpMethod.POST,
                                                             HttpMethod.PUT, HttpMethod.DELETE);

  @Name(URL)
  @Description("The URL to post data to. Additionally, a placeholder like #columnName can be added to the URL that " +
    "can be substituted with column value at the runtime. E.g. https://customer-url/user/#user_id. Here user_id " +
    "column should exist in input schema. (Macro Enabled)")
  @Macro
  private final String url;

  @Name(METHOD)
  @Description("The http request method. Defaults to POST. (Macro Enabled)")
  @Macro
  private final String method;

  @Name(BATCH_SIZE)
  @Description("Batch size. Defaults to 1. (Macro Enabled)")
  @Macro
  private final Integer batchSize;

  @Name(WRITE_JSON_AS_ARRAY)
  @Nullable
  @Description("Whether to write json as array. Defaults to false. (Macro Enabled)")
  @Macro
  private final Boolean writeJsonAsArray;

  @Name(JSON_BATCH_KEY)
  @Nullable
  @Description("Optional key to be used for wrapping json array as object. " +
    "Leave empty for no wrapping of the array (Macro Enabled)")
  @Macro
  private final String jsonBatchKey;

  @Name(DELIMETER_FOR_MESSAGE)
  @Nullable
  @Description("Delimiter for messages to be used while batching. Defaults to \"\\n\". (Macro Enabled)")
  @Macro
  private final String delimiterForMessages;

  @Name(MESSAGE_FORMAT)
  @Description("Format to send messsage in. (Macro Enabled)")
  @Macro
  private final String messageFormat;

  @Name(BODY)
  @Nullable
  @Description("Optional custom message. This is required if the message format is set to 'Custom'." +
    "User can leverage incoming message fields in the post payload. For example-" +
    "User has defined payload as \\{ \"messageType\" : \"update\", \"name\" : \"#firstName\" \\}" +
    "where #firstName will be substituted for the value that is in firstName in the incoming message. " +
    "(Macro enabled)")
  @Macro
  private final String body;

  @Name(REQUEST_HEADERS)
  @Nullable
  @Description("Request headers to set when performing the http request. (Macro enabled)")
  @Macro
  private final String requestHeaders;

  @Name(CHARSET)
  @Description("Charset. Defaults to UTF-8. (Macro enabled)")
  @Macro
  private final String charset;

  @Name(FOLLOW_REDIRECTS)
  @Description("Whether to automatically follow redirects. Defaults to true. (Macro enabled)")
  @Macro
  private final Boolean followRedirects;

  @Name(DISABLE_SSL_VALIDATION)
  @Description("If user enables SSL validation, they will be expected to add the certificate to the trustStore" +
    " on each machine. Defaults to true. (Macro enabled)")
  @Macro
  private final Boolean disableSSLValidation;

  @Nullable
  @Name(PROPERTY_HTTP_ERROR_HANDLING)
  @Description("Defines the error handling strategy to use for certain HTTP response codes." +
    "The left column contains a regular expression for HTTP status code. The right column contains an action which" +
    "is done in case of match. If HTTP status code matches multiple regular expressions, " +
    "the first specified in mapping is matched.")
  protected String httpErrorsHandling;

  @Name(PROPERTY_ERROR_HANDLING)
  @Description("Error handling strategy to use when the HTTP response cannot be transformed to an output record.")
  protected String errorHandling;

  @Name(PROPERTY_RETRY_POLICY)
  @Description("Policy used to calculate delay between retries.")
  protected String retryPolicy;

  @Nullable
  @Name(PROPERTY_LINEAR_RETRY_INTERVAL)
  @Description("Interval in seconds between retries. Is only used if retry policy is \"linear\".")
  @Macro
  protected Long linearRetryInterval;

  @Name(PROPERTY_MAX_RETRY_DURATION)
  @Description("Maximum time in seconds retries can take.")
  @Macro
  protected Long maxRetryDuration;

  @Name(CONNECTION_TIMEOUT)
  @Description("Sets the connection timeout in milliseconds. Set to 0 for infinite. Default is 60000 (1 minute). " +
    "(Macro enabled)")
  @Nullable
  @Macro
  private final Integer connectTimeout;

  @Name(READ_TIMEOUT)
  @Description("The time in milliseconds to wait for a read. Set to 0 for infinite. Defaults to 60000 (1 minute). " +
    "(Macro enabled)")
  @Nullable
  @Macro
  private final Integer readTimeout;

  public HTTPSinkConfig(String referenceName, String url, String method, Integer batchSize,
                        @Nullable String delimiterForMessages, String messageFormat, @Nullable String body,
                        @Nullable String requestHeaders, String charset,
                        boolean followRedirects, boolean disableSSLValidation, @Nullable String httpErrorsHandling,
                        String errorHandling, String retryPolicy, @Nullable Long linearRetryInterval,
                        Long maxRetryDuration, @Nullable int readTimeout, @Nullable int connectTimeout,
                        String oauth2Enabled, String authType, @Nullable String jsonBatchKey,
                        Boolean writeJsonAsArray) {
    super(referenceName);
    this.url = url;
    this.method = method;
    this.batchSize = batchSize;
    this.delimiterForMessages = delimiterForMessages;
    this.messageFormat = messageFormat;
    this.body = body;
    this.requestHeaders = requestHeaders;
    this.charset = charset;
    this.followRedirects = followRedirects;
    this.disableSSLValidation = disableSSLValidation;
    this.httpErrorsHandling = httpErrorsHandling;
    this.errorHandling = errorHandling;
    this.retryPolicy = retryPolicy;
    this.linearRetryInterval = linearRetryInterval;
    this.maxRetryDuration = maxRetryDuration;
    this.readTimeout = readTimeout;
    this.connectTimeout = connectTimeout;
    this.jsonBatchKey = jsonBatchKey;
    this.writeJsonAsArray = writeJsonAsArray;
    this.oauth2Enabled = oauth2Enabled;
    this.authType = authType;
  }

  private HTTPSinkConfig(Builder builder) {
    super(builder.referenceName);
    url = builder.url;
    method = builder.method;
    batchSize = builder.batchSize;
    delimiterForMessages = builder.delimiterForMessages;
    messageFormat = builder.messageFormat;
    body = builder.body;
    requestHeaders = builder.requestHeaders;
    charset = builder.charset;
    followRedirects = builder.followRedirects;
    disableSSLValidation = builder.disableSSLValidation;
    connectTimeout = builder.connectTimeout;
    readTimeout = builder.readTimeout;
    jsonBatchKey = builder.jsonBatchKey;
    writeJsonAsArray = builder.writeJsonAsArray;
    oauth2Enabled = builder.oauth2Enabled;
    authType = builder.authType;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(HTTPSinkConfig copy) {
    Builder builder = new Builder();
    builder.referenceName = copy.referenceName;
    builder.url = copy.getUrl();
    builder.method = copy.getMethod();
    builder.batchSize = copy.getBatchSize();
    builder.delimiterForMessages = copy.getDelimiterForMessages();
    builder.messageFormat = copy.getMessageFormat().getValue();
    builder.body = copy.getBody();
    builder.requestHeaders = copy.getRequestHeaders();
    builder.charset = copy.getCharset();
    builder.followRedirects = copy.getFollowRedirects();
    builder.disableSSLValidation = copy.getDisableSSLValidation();
    builder.connectTimeout = copy.getConnectTimeout();
    builder.readTimeout = copy.getReadTimeout();
    builder.oauth2Enabled = copy.getOAuth2Enabled();
    builder.authType = copy.getAuthTypeString();
    return builder;
  }

  public String getUrl() {
    return url;
  }

  public String getMethod() {
    return method;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public boolean shouldWriteJsonAsArray() {
    return writeJsonAsArray != null && writeJsonAsArray;
  }

  public String getJsonBatchKey() {
    return jsonBatchKey;
  }

  @Nullable
  public String getDelimiterForMessages() {
    return Strings.isNullOrEmpty(delimiterForMessages) ? "\n" : delimiterForMessages;
  }

  public MessageFormatType getMessageFormat() {
    return MessageFormatType.valueOf(messageFormat.toUpperCase());
  }

  @Nullable
  public String getBody() {
    return body;
  }

  @Nullable
  public String getRequestHeaders() {
    return requestHeaders;
  }

  public String getCharset() {
    return charset;
  }

  public Boolean getFollowRedirects() {
    return followRedirects;
  }

  public Boolean getDisableSSLValidation() {
    return disableSSLValidation;
  }

  @Nullable
  public String getHttpErrorsHandling() {
    return httpErrorsHandling;
  }

  public ErrorHandling getErrorHandling() {
    return getEnumValueByString(ErrorHandling.class, errorHandling, PROPERTY_ERROR_HANDLING);
  }

  public RetryPolicy getRetryPolicy() {
    return getEnumValueByString(RetryPolicy.class, retryPolicy, PROPERTY_RETRY_POLICY);
  }

  private static <T extends EnumWithValue> T
  getEnumValueByString(Class<T> enumClass, String stringValue, String propertyName) {
    return Stream.of(enumClass.getEnumConstants())
      .filter(keyType -> keyType.getValue().equalsIgnoreCase(stringValue))
      .findAny()
      .orElseThrow(() -> new InvalidConfigPropertyException(
        String.format("Unsupported value for '%s': '%s'", propertyName, stringValue), propertyName));
  }

  @Nullable
  public Long getLinearRetryInterval() {
    return linearRetryInterval;
  }

  public Long getMaxRetryDuration() {
    return maxRetryDuration;
  }

  @Nullable
  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  @Nullable
  public Integer getReadTimeout() {
    return readTimeout;
  }

  public Map<String, String> getRequestHeadersMap() {
    return convertHeadersToMap(requestHeaders);
  }

  public Map<String, String> getHeadersMap(String header) {
    return convertHeadersToMap(header);
  }

  public String getReferenceNameOrNormalizedFQN() {
    return Strings.isNullOrEmpty(referenceName) ? ReferenceNames.normalizeFqn(url) : referenceName;
  }

  public List<HttpErrorHandlerEntity> getHttpErrorHandlingEntries() {
    Map<String, String> httpErrorsHandlingMap = getMapFromKeyValueString(httpErrorsHandling);
    List<HttpErrorHandlerEntity> results = new ArrayList<>(httpErrorsHandlingMap.size());

    for (Map.Entry<String, String> entry : httpErrorsHandlingMap.entrySet()) {
      String regex = entry.getKey();
      try {
        results.add(new HttpErrorHandlerEntity(Pattern.compile(regex),
                                               getEnumValueByString(RetryableErrorHandling.class,
                                                                    entry.getValue(), PROPERTY_HTTP_ERROR_HANDLING)));
      } catch (PatternSyntaxException e) {
        // We embed causing exception message into this one. Since this message is shown on UI when validation fails.
        throw new InvalidConfigPropertyException(
          String.format(
            "Error handling regex '%s' is not valid. %s", regex, e.getMessage()), PROPERTY_HTTP_ERROR_HANDLING);
      }
    }
    return results;
  }

  public static Map<String, String> getMapFromKeyValueString(String keyValueString) {
    Map<String, String> result = new LinkedHashMap<>();

    if (Strings.isNullOrEmpty(keyValueString)) {
      return result;
    }

    String[] mappings = keyValueString.split(",");
    for (String map : mappings) {
      String[] columns = map.split(":");
      if (columns.length < 2) { //For scenario where either of key or value not provided
        throw new IllegalArgumentException(String.format("Missing value for key %s", columns[0]));
      }
      result.put(columns[0], columns[1]);
    }
    return result;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);

    if (!containsMacro(URL)) {
      try {
        new URL(url);
      } catch (MalformedURLException e) {
        collector.addFailure(String.format("URL '%s' is malformed: %s", url, e.getMessage()), null)
          .withConfigProperty(URL);
      }
    }

    if (!containsMacro(CONNECTION_TIMEOUT) && Objects.nonNull(connectTimeout) && connectTimeout < 0) {
      collector.addFailure("Connection Timeout cannot be a negative number.", null)
        .withConfigProperty(CONNECTION_TIMEOUT);
    }

    try {
      convertHeadersToMap(requestHeaders);
    } catch (IllegalArgumentException e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(REQUEST_HEADERS);
    }

    if (!containsMacro(METHOD) && !METHODS.contains(method.toUpperCase())) {
      collector.addFailure(
        String.format("Invalid request method %s, must be one of %s.", method, Joiner.on(',').join(METHODS)), null)
        .withConfigProperty(METHOD);
    }

    if (!containsMacro(BATCH_SIZE) && batchSize != null && batchSize < 1) {
      collector.addFailure("Batch size must be greater than 0.", null)
        .withConfigProperty(BATCH_SIZE);
    }

    // Validate Linear Retry Interval
    if (!containsMacro(PROPERTY_RETRY_POLICY) && getRetryPolicy() == RetryPolicy.LINEAR) {
      assertIsSet(getLinearRetryInterval(), PROPERTY_LINEAR_RETRY_INTERVAL, "retry policy is linear");
    }

    if (!containsMacro(READ_TIMEOUT) && Objects.nonNull(readTimeout) && readTimeout < 0) {
      collector.addFailure("Read Timeout cannot be a negative number.", null)
        .withConfigProperty(READ_TIMEOUT);
    }

    if (!containsMacro(MESSAGE_FORMAT) && !containsMacro("body") && messageFormat.equalsIgnoreCase("Custom")
      && body == null) {
      collector.addFailure("For Custom message format, message cannot be null.", null)
        .withConfigProperty(MESSAGE_FORMAT);
    }
  }

  public void validateSchema(@Nullable Schema schema, FailureCollector collector) {
    if (schema == null) {
      return;
    }
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      collector.addFailure("Schema must contain at least one field", null);
      throw collector.getOrThrowException();
    }
    
    if ((method.equals("PUT") || method.equals("DELETE")) && url.contains(PLACEHOLDER_FORMAT)) {
      Pattern pattern = Pattern.compile(REGEX_HASHED_VAR);
      Matcher matcher = pattern.matcher(url);
      while (matcher.find()) {
        if (!fields.contains(matcher.group(1))) {
          collector.addFailure("Schema must contain all fields mentioned in the url", null);
        }
      }
    }
  }

  private Map<String, String> convertHeadersToMap(String headersString) {
    Map<String, String> headersMap = new HashMap<>();
    if (!Strings.isNullOrEmpty(headersString)) {
      for (String chunk : headersString.split(DELIMITER)) {
        String[] keyValue = chunk.split(KV_DELIMITER, 2);
        if (keyValue.length == 2) {
          headersMap.put(keyValue[0], keyValue[1]);
        } else {
          throw new IllegalArgumentException(String.format("Unable to parse key-value pair '%s'.", chunk));
        }
      }
    }
    return headersMap;
  }

  /**
   * Builder for creating a {@link HTTPSinkConfig}.
   */
  public static final class Builder {
    private String referenceName;
    private String url;
    private String method;
    private Integer batchSize;
    private Boolean writeJsonAsArray;
    private String jsonBatchKey;
    private String delimiterForMessages;
    private String messageFormat;
    private String body;
    private String requestHeaders;
    private String charset;
    private Boolean followRedirects;
    private Boolean disableSSLValidation;
    private Integer connectTimeout;
    private Integer readTimeout;
    private String oauth2Enabled;
    private String authType;

    private Builder() {
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setUrl(String url) {
      this.url = url;
      return this;
    }

    public Builder setMethod(String method) {
      this.method = method;
      return this;
    }

    public Builder setBatchSize(Integer batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder setWriteJsonAsArray(Boolean writeJsonAsArray) {
      this.writeJsonAsArray = writeJsonAsArray;
      return this;
    }

    public Builder setJsonBatchKey(String jsonBatchKey) {
      this.jsonBatchKey = jsonBatchKey;
      return this;
    }

    public Builder setDelimiterForMessages(String delimiterForMessages) {
      this.delimiterForMessages = delimiterForMessages;
      return this;
    }

    public Builder setMessageFormat(String messageFormat) {
      this.messageFormat = messageFormat;
      return this;
    }

    public Builder setBody(String body) {
      this.body = body;
      return this;
    }

    public Builder setRequestHeaders(String requestHeaders) {
      this.requestHeaders = requestHeaders;
      return this;
    }

    public Builder setCharset(String charset) {
      this.charset = charset;
      return this;
    }

    public Builder setFollowRedirects(Boolean followRedirects) {
      this.followRedirects = followRedirects;
      return this;
    }

    public Builder setDisableSSLValidation(Boolean disableSSLValidation) {
      this.disableSSLValidation = disableSSLValidation;
      return this;
    }

    public Builder setConnectTimeout(Integer connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder setReadTimeout(Integer readTimeout) {
      this.readTimeout = readTimeout;
      return this;
    }

    public HTTPSinkConfig build() {
      return new HTTPSinkConfig(this);
    }
  }
}
