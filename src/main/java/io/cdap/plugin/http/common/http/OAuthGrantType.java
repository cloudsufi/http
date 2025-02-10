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
 * Enum encoding the handled Oauth2 Grant Types
 */
public enum OAuthGrantType implements EnumWithValue {
  REFRESH_TOKEN("refresh token", "Refresh Token"), CLIENT_CREDENTIALS("client_credentials",
      "Client Credentials");

  private final String value;
  private final String label;

  OAuthGrantType(String value, String label) {
    this.value = value;
    this.label = label;
  }

  public static OAuthGrantType getGrantType(String oauth2GrantType) {
    if (Objects.equals(oauth2GrantType, REFRESH_TOKEN.getLabel())) {
      return REFRESH_TOKEN;
    } else {
      return CLIENT_CREDENTIALS;
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
