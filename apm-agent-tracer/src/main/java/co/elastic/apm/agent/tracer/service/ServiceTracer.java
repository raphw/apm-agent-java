/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package co.elastic.apm.agent.tracer.service;

import co.elastic.apm.agent.tracer.ErrorCapture;
import co.elastic.apm.agent.tracer.Span;
import co.elastic.apm.agent.tracer.Tracer;
import co.elastic.apm.agent.tracer.Transaction;

import javax.annotation.Nullable;

public interface ServiceTracer extends Tracer {

    @Nullable
    ServiceInfo getServiceInfoForClassLoader(@Nullable ClassLoader classLoader);

    /**
     * Sets the service name and version for all {@link Transaction}s,
     * {@link Span}s and {@link ErrorCapture}s which are created by the service which corresponds to the provided {@link ClassLoader}.
     * <p>
     * The main use case is being able to differentiate between multiple services deployed to the same application server.
     * </p>
     *
     * @param classLoader the class loader which corresponds to a particular service
     * @param serviceInfo the service name and version for this class loader
     */
    void setServiceInfoForClassLoader(@Nullable ClassLoader classLoader, ServiceInfo serviceInfo);

    ServiceInfo autoDetectedServiceInfo();
}
