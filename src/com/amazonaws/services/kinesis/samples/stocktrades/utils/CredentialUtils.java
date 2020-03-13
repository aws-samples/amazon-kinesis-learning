/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.stocktrades.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

/**
 * Provides utilities for retrieving credentials to talk to AWS
 */
public class CredentialUtils {

    public static AWSCredentialsProvider getCredentialsProvider() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default] credential profile by
         * reading from the credentials file located at (~/.aws/credentials).
         */
        AWSCredentialsProvider credentialsProvider = null;
        try {
            credentialsProvider = new ProfileCredentialsProvider("default");
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
        return credentialsProvider;
    }

}
