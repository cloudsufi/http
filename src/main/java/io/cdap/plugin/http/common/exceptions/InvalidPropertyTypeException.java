package io.cdap.plugin.http.common.exceptions;

import java.util.List;

/**
 * Indicates illegal property.
 */
public class InvalidPropertyTypeException extends RuntimeException {

  public InvalidPropertyTypeException(String propertyLabel, String value, List<String> allowedValues) {
    super(String.format("'%s' is not a value for '%s' property. Allowed values are: '%s'.", value, propertyLabel,
      String.join(", ", allowedValues)));
  }
}

