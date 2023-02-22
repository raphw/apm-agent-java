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
package co.elastic.apm.agent.impl;

import co.elastic.apm.agent.configuration.ServiceInfo;
import co.elastic.apm.agent.impl.error.ErrorCapture;
import co.elastic.apm.agent.impl.sampling.Sampler;
import co.elastic.apm.agent.impl.transaction.AbstractSpan;
import co.elastic.apm.agent.impl.transaction.BinaryHeaderGetter;
import co.elastic.apm.agent.impl.transaction.Span;
import co.elastic.apm.agent.impl.transaction.TextHeaderGetter;
import co.elastic.apm.agent.impl.transaction.Transaction;
import co.elastic.apm.agent.objectpool.ObjectPoolFactory;
import co.elastic.apm.agent.util.PrivilegedActionUtils;
import co.elastic.apm.agent.util.VersionUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

public class GlobalTracer implements Tracer {

    private static final GlobalTracer INSTANCE = new GlobalTracer();
    private volatile Tracer tracer = NoopTracer.INSTANCE;
    private static volatile boolean classloaderCheckOk = false;

    private GlobalTracer() {
    }

    static {
        checkClassloader();
    }

    public static Tracer get() {
        return INSTANCE;
    }

    @Nullable
    public static <T extends Tracer> T get(Class<T> type) {
        Tracer tracer = INSTANCE.tracer;
        if (type.isInstance(tracer)) {
            return type.cast(tracer);
        }
        return null;
    }

    public static <T extends Tracer> T require(Class<T> type) {
        return Objects.requireNonNull(get(type), "Registered tracer is not an instance of " + type.getName());
    }

    @Nullable
    public static ElasticApmTracer getTracerImpl() {
        return get(ElasticApmTracer.class);
    }

    public static ElasticApmTracer requireTracerImpl() {
        return Objects.requireNonNull(getTracerImpl(), "Registered tracer is not an instance of ElasticApmTracer");
    }

    private static void checkClassloader() {
        ClassLoader cl = PrivilegedActionUtils.getClassLoader(GlobalTracer.class);

        // agent currently loaded in the bootstrap CL, which is the current correct location
        if (cl == null) {
            return;
        }

        if (classloaderCheckOk) {
            return;
        }

        String agentLocation = PrivilegedActionUtils.getProtectionDomain(GlobalTracer.class).getCodeSource().getLocation().getFile();
        if (!agentLocation.endsWith(".jar")) {
            // agent is not packaged, thus we assume running tests
            classloaderCheckOk = true;
            return;
        }

        String premainClass = VersionUtils.getManifestEntry(new File(agentLocation), "Premain-Class");
        if (null == premainClass) {
            // packaged within a .jar, but not within an agent jar, thus we assume it's still for testing
            classloaderCheckOk = true;
            return;
        }

        if (premainClass.startsWith("co.elastic.apm.agent")) {
            // premain class will only be present when packaged as an agent jar
            classloaderCheckOk = true;
            return;
        }

        // A packaged agent class has been loaded outside of bootstrap classloader, we are not in the context of
        // unit/integration tests, that's likely a setup issue where the agent jar has been added to application
        // classpath.
        throw new IllegalStateException(String.format("Agent setup error: agent jar file \"%s\"  likely referenced in JVM or application classpath", agentLocation));

    }

    public static synchronized void setNoop() {
        TracerState currentTracerState = INSTANCE.tracer.getState();
        if (currentTracerState != TracerState.UNINITIALIZED && currentTracerState != TracerState.STOPPED) {
            throw new IllegalStateException("Can't override tracer as current tracer is already running");
        }
        INSTANCE.tracer = NoopTracer.INSTANCE;
    }

    public static synchronized void init(Tracer tracer) {
        if (!isNoop()) {
            throw new IllegalStateException("Tracer is already initialized");
        }
        co.elastic.apm.plugin.spi.GlobalTracer.init(tracer);
        INSTANCE.tracer = tracer;
    }

    public static boolean isNoop() {
        return INSTANCE.tracer == NoopTracer.INSTANCE;
    }

    @Nullable
    public Transaction startRootTransaction(@Nullable ClassLoader initiatingClassLoader) {
        return tracer.startRootTransaction(initiatingClassLoader);
    }

    @Nullable
    @Override
    public Transaction startRootTransaction(@Nullable ClassLoader initiatingClassLoader, long epochMicro) {
        return tracer.startRootTransaction(initiatingClassLoader, epochMicro);
    }

    @Nullable
    public Transaction startRootTransaction(Sampler sampler, long epochMicros, @Nullable ClassLoader initiatingClassLoader) {
        return tracer.startRootTransaction(sampler, epochMicros, initiatingClassLoader);
    }

    @Nullable
    public <C> Transaction startChildTransaction(@Nullable C headerCarrier, TextHeaderGetter<C> textHeadersGetter, @Nullable ClassLoader initiatingClassLoader) {
        return tracer.startChildTransaction(headerCarrier, textHeadersGetter, initiatingClassLoader);
    }

    @Nullable
    @Override
    public <C> Transaction startChildTransaction(@Nullable C headerCarrier, TextHeaderGetter<C> textHeadersGetter, @Nullable ClassLoader initiatingClassLoader, long epochMicros) {
        return tracer.startChildTransaction(headerCarrier, textHeadersGetter, initiatingClassLoader, epochMicros);
    }

    @Nullable
    public <C> Transaction startChildTransaction(@Nullable C headerCarrier, TextHeaderGetter<C> textHeadersGetter, Sampler sampler, long epochMicros, @Nullable ClassLoader initiatingClassLoader) {
        return tracer.startChildTransaction(headerCarrier, textHeadersGetter, sampler, epochMicros, initiatingClassLoader);
    }

    @Nullable
    public <C> Transaction startChildTransaction(@Nullable C headerCarrier, BinaryHeaderGetter<C> binaryHeadersGetter, @Nullable ClassLoader initiatingClassLoader) {
        return tracer.startChildTransaction(headerCarrier, binaryHeadersGetter, initiatingClassLoader);
    }

    @Nullable
    public <C> Transaction startChildTransaction(@Nullable C headerCarrier, BinaryHeaderGetter<C> binaryHeadersGetter, Sampler sampler, long epochMicros, @Nullable ClassLoader initiatingClassLoader) {
        return tracer.startChildTransaction(headerCarrier, binaryHeadersGetter, sampler, epochMicros, initiatingClassLoader);
    }

    @Nullable
    public Transaction currentTransaction() {
        return tracer.currentTransaction();
    }

    @Nullable
    @Override
    public AbstractSpan<?> getActive() {
        return tracer.getActive();
    }

    @Nullable
    @Override
    public Span getActiveSpan() {
        return tracer.getActiveSpan();
    }

    public void captureAndReportException(@Nullable Throwable e, ClassLoader initiatingClassLoader) {
        tracer.captureAndReportException(e, initiatingClassLoader);
    }

    @Nullable
    public String captureAndReportException(long epochMicros, @Nullable Throwable e, @Nullable AbstractSpan<?> parent) {
        return tracer.captureAndReportException(epochMicros, e, parent);
    }

    @Nullable
    public ErrorCapture captureException(@Nullable Throwable e, @Nullable AbstractSpan<?> parent, @Nullable ClassLoader initiatingClassLoader) {
        return tracer.captureException(e, parent, initiatingClassLoader);
    }

    @Nullable
    @Override
    public Span getActiveExitSpan() {
        return tracer.getActiveExitSpan();
    }

    @Override
    public TracerState getState() {
        return tracer.getState();
    }

    @Nullable
    @Override
    public ServiceInfo getServiceInfoForClassLoader(@Nullable ClassLoader classLoader) {
        return tracer.getServiceInfoForClassLoader(classLoader);
    }

    @Override
    public void setServiceInfoForClassLoader(@Nullable ClassLoader classLoader, ServiceInfo serviceInfo) {
        tracer.setServiceInfoForClassLoader(classLoader, serviceInfo);
    }

    @Override
    public void stop() {
        tracer.stop();
    }

    @Override
    public boolean isRunning() {
        return tracer.isRunning();
    }

    @Nullable
    @Override
    public Span createExitChildSpan() {
        return tracer.createExitChildSpan();
    }

    @Override
    public void recycle(Transaction transaction) {
        tracer.recycle(transaction);
    }

    @Override
    public void endSpan(Span span) {
        tracer.endSpan(span);
    }

    @Override
    public void endTransaction(Transaction transaction) {
        tracer.endTransaction(transaction);
    }

    @Override
    public void endError(ErrorCapture errorCapture) {
        tracer.endError(errorCapture);
    }

    @Override
    public <T> T getConfig(Class<T> configuration) {
        return tracer.getConfig(configuration);
    }

    @Override
    public ObjectPoolFactory getObjectPoolFactory() {
        return tracer.getObjectPoolFactory();
    }

    @Override
    public void recycle(ErrorCapture errorCapture) {
        tracer.recycle(errorCapture);
    }

    @Nullable
    @Override
    public <C> co.elastic.apm.plugin.spi.Transaction<?> startChildTransaction(@Nullable C headerCarrier, co.elastic.apm.plugin.spi.TextHeaderGetter<C> textHeadersGetter, @Nullable ClassLoader initiatingClassLoader) {
        return tracer.startChildTransaction(headerCarrier, textHeadersGetter, initiatingClassLoader);
    }

    @Nullable
    @Override
    public <C> co.elastic.apm.plugin.spi.Transaction<?> startChildTransaction(@Nullable C headerCarrier, co.elastic.apm.plugin.spi.TextHeaderGetter<C> textHeadersGetter, @Nullable ClassLoader initiatingClassLoader, long epochMicros) {
        return tracer.startChildTransaction(headerCarrier, textHeadersGetter, initiatingClassLoader, epochMicros);
    }

    @Nullable
    @Override
    public <C> co.elastic.apm.plugin.spi.Transaction<?> startChildTransaction(@Nullable C headerCarrier, co.elastic.apm.plugin.spi.BinaryHeaderGetter<C> binaryHeadersGetter, @Nullable ClassLoader initiatingClassLoader) {
        return tracer.startChildTransaction(headerCarrier, binaryHeadersGetter, initiatingClassLoader);
    }

    @Nullable
    @Override
    public String captureAndReportException(long epochMicros, @Nullable Throwable e, @Nullable co.elastic.apm.plugin.spi.AbstractSpan<?> parent) {
        return tracer.captureAndReportException(epochMicros, e, parent);
    }

    @Nullable
    @Override
    public co.elastic.apm.plugin.spi.ErrorCapture captureException(@Nullable Throwable e, @Nullable co.elastic.apm.plugin.spi.AbstractSpan<?> parent, @Nullable ClassLoader initiatingClassLoader) {
        return tracer.captureException(e, parent, initiatingClassLoader);
    }

    @Override
    public void endSpan(co.elastic.apm.plugin.spi.Span<?> span) {
        tracer.endSpan(span);
    }

    @Override
    public void endTransaction(co.elastic.apm.plugin.spi.Transaction<?> transaction) {
        tracer.endTransaction(transaction);
    }

    @Override
    public void endError(co.elastic.apm.plugin.spi.ErrorCapture errorCapture) {
        tracer.endError(errorCapture);
    }

    @Override
    public void setServiceInfoForClassLoader(ClassLoader classLoader, co.elastic.apm.plugin.spi.ServiceInfo serviceInfo) {
        tracer.setServiceInfoForClassLoader(classLoader, serviceInfo);
    }

    @Override
    public ServiceInfo autoDetectedServiceName() {
        return tracer.autoDetectedServiceName();
    }
}
