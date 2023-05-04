/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.invoker.lambda;

import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.utils.AttributeMap;

import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import java.time.Duration;

/**
 * @author hank
 * @date 4/18/22
 */
public class Lambda
{
    private static final Lambda instance = new Lambda();
    
    final AttributeMap attributeMap = AttributeMap.builder()
    .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
    .build();

    public static Lambda Instance()
    {
        return instance;
    }

    private final LambdaAsyncClient asyncClient;

    private Lambda()
    {   
        /* under proxy */
        // asyncClient = LambdaAsyncClient.builder().httpClient(
        //         AwsCrtAsyncHttpClient.builder().maxConcurrency(1000)
        //                 .connectionMaxIdleTime(Duration.ofSeconds(1000)).buildWithDefaults(attributeMap)).build();
        
        /* under normal */
        asyncClient = LambdaAsyncClient.builder().httpClientBuilder(
                AwsCrtAsyncHttpClient.builder().maxConcurrency(1000)
                        .connectionMaxIdleTime(Duration.ofSeconds(1000))).build();
    }

    public LambdaAsyncClient getAsyncClient()
    {
        return asyncClient;
    }
}
