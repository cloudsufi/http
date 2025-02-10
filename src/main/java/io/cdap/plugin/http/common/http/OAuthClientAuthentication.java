/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.plugin.http.common.http;

import io.cdap.plugin.http.common.EnumWithValue;
import java.util.Objects;

/**
 * Enum encoding the handled Oauth2 Client Authentication
 */
public enum OAuthClientAuthentication implements EnumWithValue {
  BODY("body", "Body"),
  REQUEST_PARAMETER("request_parameter", "Request Parameter");

  private final String value;
  private final String label;

  OAuthClientAuthentication(String value, String label) {
    this.value = value;
    this.label = label;
  }

  public static OAuthClientAuthentication getClientAuthentication(String clientAuthentication) {
    if (Objects.equals(clientAuthentication, BODY.getLabel())) {
      return BODY;
    } else {
      return REQUEST_PARAMETER;
    }
  }

  @Override
  public String getValue() {
    return value;
  }

  public String getLabel() {
    return label;
  }

  @Override
  public String toString() {
    return this.getValue();
  }
}

