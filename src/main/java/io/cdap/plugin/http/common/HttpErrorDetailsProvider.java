/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.http.common;

import io.cdap.cdap.api.exception.*;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProvider;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;

import java.io.*;
import java.util.List;

import java.util.NoSuchElementException;

public class HttpErrorDetailsProvider implements ErrorDetailsProvider {
    @Override
    public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
        List<Throwable> causalChain = Throwables.getCausalChain(e);
        for (Throwable t : causalChain) {
            //, UnsupportedOperationException,
            if (t instanceof ProgramFailureException) {
                // if causal chain already has program failure exception, return null to avoid double wrap.
                return null;
            }
            if (t instanceof IllegalArgumentException) {
                return getProgramFailureException((IllegalArgumentException) t, errorContext);
            }
            if (t instanceof IllegalStateException) {
                return getProgramFailureException((IllegalStateException) t, errorContext);
            }
            if (t instanceof InvalidConfigPropertyException) {
                return getProgramFailureException((InvalidConfigPropertyException) t, errorContext);
            }
            if (t instanceof NoSuchElementException) {
                return getProgramFailureException((NoSuchElementException) t, errorContext);
            }
            if (t instanceof UnsupportedEncodingException) {
                return getProgramFailureException((UnsupportedEncodingException) t, errorContext);
            }
        }
        return null;
    }

    /**
     * Get a ProgramFailureException with the given error
     * information from {@link IllegalArgumentException}.
     *
     * @param e The IllegalArgumentException to get the error information from.
     * @return A ProgramFailureException with the given error information.
     */
    private ProgramFailureException getProgramFailureException(IllegalArgumentException e, ErrorContext errorContext) {
        String errorMessage = e.getMessage();
        String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";
        return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN), errorMessage,
                String.format(errorMessageFormat, errorContext.getPhase(), errorMessage), ErrorType.USER, false, e);
    }

    /**
     * Get a ProgramFailureException with the given error
     * information from {@link IllegalStateException}.
     *
     * @param e The IllegalStateException to get the error information from.
     * @return A ProgramFailureException with the given error information.
     */
    private ProgramFailureException getProgramFailureException(IllegalStateException e, ErrorContext errorContext) {
        String errorMessage = e.getMessage();
        String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";
        return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN), errorMessage,
                String.format(errorMessageFormat, errorContext.getPhase(), errorMessage), ErrorType.SYSTEM, false, e);
    }

    /**
     * Get a ProgramFailureException with the given error
     * information from {@link InvalidConfigPropertyException}.
     *
     * @param e The InvalidConfigPropertyException to get the error information from.
     * @return A ProgramFailureException with the given error information.
     */
    private ProgramFailureException getProgramFailureException(InvalidConfigPropertyException e, ErrorContext errorContext) {
        String errorMessage = e.getMessage();
        String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";
        return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN), errorMessage,
                String.format(errorMessageFormat, errorContext.getPhase(), errorMessage), ErrorType.SYSTEM, false, e);
    }

    /**
     * Get a ProgramFailureException with the given error
     * information from {@link NoSuchElementException}.
     *
     * @param e The NoSuchElementException to get the error information from.
     * @return A ProgramFailureException with the given error information.
     */
    private ProgramFailureException getProgramFailureException(NoSuchElementException e, ErrorContext errorContext) {
        String errorMessage = e.getMessage();
        String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";
        return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN), errorMessage,
                String.format(errorMessageFormat, errorContext.getPhase(), errorMessage), ErrorType.SYSTEM, false, e);
    }


    /**
     * Get a ProgramFailureException with the given error
     * information from {@link UnsupportedEncodingException}.
     *
     * @param e The UnsupportedEncodingException to get the error information from.
     * @return A ProgramFailureException with the given error information.
     */
    private ProgramFailureException getProgramFailureException(UnsupportedEncodingException e, ErrorContext errorContext) {
        String errorMessage = e.getMessage();
        String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";
        return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN), errorMessage,
                String.format(errorMessageFormat, errorContext.getPhase(), errorMessage), ErrorType.SYSTEM, false, e);
    }


}
