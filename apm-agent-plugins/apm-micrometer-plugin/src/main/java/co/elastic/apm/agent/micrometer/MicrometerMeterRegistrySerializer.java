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
package co.elastic.apm.agent.micrometer;

import co.elastic.apm.agent.sdk.internal.util.PrivilegedActionUtils;
import co.elastic.apm.agent.sdk.logging.Logger;
import co.elastic.apm.agent.sdk.logging.LoggerFactory;
import co.elastic.apm.agent.sdk.weakconcurrent.WeakConcurrent;
import co.elastic.apm.agent.sdk.weakconcurrent.WeakSet;
import co.elastic.apm.agent.tracer.configuration.MetricsConfiguration;
import co.elastic.apm.agent.tracer.reporting.ReportWriter;
import co.elastic.apm.agent.tracer.reporting.ReportingTracer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.CountAtBucket;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MicrometerMeterRegistrySerializer {

    private static final int BUFFER_SIZE_LIMIT = 2048;

    private static final Logger logger = LoggerFactory.getLogger(MicrometerMeterRegistrySerializer.class);
    private final ReportingTracer tracer;
    private final MetricsConfiguration config;
    private final WeakSet<Meter> internallyDisabledMeters = WeakConcurrent.buildSet();

    private int maxSerializedSize = 512;

    public MicrometerMeterRegistrySerializer(MetricsConfiguration config, ReportingTracer tracer) {
        this.config = config;
        this.tracer = tracer;
    }

    Iterable<Meter> getFailedMeters() {
        return internallyDisabledMeters;
    }

    public List<ReportWriter> writer(final Map<Meter.Id, Meter> metersById, final long epochMicros) {
        List<ReportWriter> serializedMeters = new ArrayList<>();
        final Map<List<Tag>, List<Meter>> metersGroupedByTags = new HashMap<>();
        for (Map.Entry<Meter.Id, Meter> entry : metersById.entrySet()) {
            List<Tag> tags = entry.getKey().getTags();
            List<Meter> meters = metersGroupedByTags.get(tags);
            if (meters == null) {
                meters = new ArrayList<>(1);
                metersGroupedByTags.put(tags, meters);
            }
            meters.add(entry.getValue());
        }
        for (Map.Entry<List<Tag>, List<Meter>> entry : metersGroupedByTags.entrySet()) {
            ReportWriter meter = tracer.newWriter(maxSerializedSize);
            if (serializeMetricSet(entry.getKey(), entry.getValue(), epochMicros, meter)) {
                serializedMeters.add(meter);
                maxSerializedSize = Math.max(Math.min(meter.size(), BUFFER_SIZE_LIMIT), maxSerializedSize);
            }
        }
        return serializedMeters;
    }

    boolean serializeMetricSet(List<Tag> tags, List<Meter> meters, long epochMicros, ReportWriter writer) {
        boolean hasSamples = false;
        boolean dedotMetricName = config.isDedotCustomMetrics();
        writer.writeObjectStart();
        {
            writer.writeFieldStart("metricset");
            writer.writeObjectStart();
            {
                writer.writeFieldStart("timestamp");
                writer.writeFieldValue(epochMicros);
                writer.writeSeparator();
                serializeTags(tags, writer);
                writer.writeFieldStart("samples");
                writer.writeObjectStart();

                ClassLoader originalContextCL = PrivilegedActionUtils.getContextClassLoader(Thread.currentThread());
                try {
                    for (int i = 0, size = meters.size(); i < size; i++) {
                        Meter meter = meters.get(i);
                        if (internallyDisabledMeters.contains(meter)) {
                            continue;
                        }
                        try {
                            // Setting the Meter CL as the context class loader during the Meter query operations
                            PrivilegedActionUtils.setContextClassLoader(Thread.currentThread(), PrivilegedActionUtils.getClassLoader(meter.getClass()));
                            if (meter instanceof Timer) {
                                Timer timer = (Timer) meter;
                                hasSamples = serializeTimer(writer, timer.takeSnapshot(), timer.getId(), timer.count(), timer.totalTime(TimeUnit.MICROSECONDS), hasSamples, dedotMetricName);
                            } else if (meter instanceof FunctionTimer) {
                                FunctionTimer timer = (FunctionTimer) meter;
                                hasSamples = serializeTimer(writer, null, timer.getId(), (long) timer.count(), timer.totalTime(TimeUnit.MICROSECONDS), hasSamples, dedotMetricName);
                            } else if (meter instanceof LongTaskTimer) {
                                LongTaskTimer timer = (LongTaskTimer) meter;
                                hasSamples = serializeTimer(writer, timer.takeSnapshot(), timer.getId(), timer.activeTasks(), timer.duration(TimeUnit.MICROSECONDS), hasSamples, dedotMetricName);
                            } else if (meter instanceof DistributionSummary) {
                                DistributionSummary summary = (DistributionSummary) meter;
                                hasSamples = serializeDistributionSummary(writer, summary.takeSnapshot(), summary.getId(), summary.count(), summary.totalAmount(), hasSamples, dedotMetricName);
                            } else if (meter instanceof Gauge) {
                                Gauge gauge = (Gauge) meter;
                                hasSamples = serializeValue(gauge.getId(), gauge.value(), hasSamples, writer, dedotMetricName);
                            } else if (meter instanceof Counter) {
                                Counter counter = (Counter) meter;
                                hasSamples = serializeValue(counter.getId(), counter.count(), hasSamples, writer, dedotMetricName);
                            } else if (meter instanceof FunctionCounter) {
                                FunctionCounter counter = (FunctionCounter) meter;
                                hasSamples = serializeValue(counter.getId(), counter.count(), hasSamples, writer, dedotMetricName);
                            }
                        } catch (Throwable throwable) {
                            String meterName = meter.getId().getName();
                            logger.warn("Failed to serialize Micrometer meter \"{}\" with tags {}. This meter will be " +
                                "excluded from serialization going forward.", meterName, tags);
                            logger.debug("Detailed info about failure to register Micrometer meter \"" + meterName +
                                "\": ", throwable);
                            internallyDisabledMeters.add(meter);
                        }
                    }
                } finally {
                    PrivilegedActionUtils.setContextClassLoader(Thread.currentThread(), originalContextCL);
                }
                writer.writeObjectEnd();
            }
            writer.writeObjectEnd();
        }
        writer.writeObjectEnd();
        writer.writeNewLine();
        return hasSamples;
    }

    private static void serializeTags(List<Tag> tags, ReportWriter writer) {
        if (tags.isEmpty()) {
            return;
        }
        writer.writeFieldStart("tags");
        writer.writeObjectStart();
        for (int i = 0, tagsSize = tags.size(); i < tagsSize; i++) {
            Tag tag = tags.get(i);
            if (i > 0) {
                writer.writeSeparator();
            }
            writer.writeFieldStartSanitized(tag.getKey());
            writer.writeFieldValue(tag.getValue());
        }
        writer.writeObjectEnd();
        writer.writeSeparator();
    }

    /**
     * Conditionally serializes a {@link Timer} if the total time is valid, i.e. neither Double.NaN nor +/-Infinite
     *
     * @param writer        writer
     * @param histogramSnapshot
     * @param id        meter ID
     * @param count     count
     * @param totalTime total time
     * @param hasValue  whether a value has already been written
     * @param dedotMetricName
     * @return true if a value has been written before, including this one; false otherwise
     */
    private static boolean serializeTimer(ReportWriter writer, HistogramSnapshot histogramSnapshot, Meter.Id id, long count, double totalTime, boolean hasValue, boolean dedotMetricName) {
        if (isValidValue(totalTime)) {
            if (hasValue) writer.writeSeparator();
            serializeValue(id, ".count", count, writer, dedotMetricName);
            writer.writeSeparator();
            serializeValue(id, ".sum.us", totalTime, writer, dedotMetricName);
            if (histogramSnapshot != null && histogramSnapshot.histogramCounts().length > 0) {
                writer.writeSeparator();
                serializeHistogram(id, histogramSnapshot, writer, dedotMetricName);
            }
            return true;
        }
        return hasValue;
    }

    /**
     * Conditionally serializes a {@link DistributionSummary} if the total amount is valid, i.e. neither Double.NaN nor +/-Infinite
     *
     * @param writer          writer
     * @param histogramSnapshot
     * @param id          meter ID
     * @param count       count
     * @param totalAmount total amount of recorded events
     * @param hasValue    whether a value has already been written
     * @param dedotMetricName
     * @return true if a value has been written before, including this one; false otherwise
     */
    private static boolean serializeDistributionSummary(ReportWriter writer, HistogramSnapshot histogramSnapshot, Meter.Id id, long count, double totalAmount, boolean hasValue, boolean dedotMetricName) {
        if (isValidValue(totalAmount)) {
            if (hasValue) writer.writeSeparator();
            serializeValue(id, ".count", count, writer, dedotMetricName);
            writer.writeSeparator();
            serializeValue(id, ".sum", totalAmount, writer, dedotMetricName);
            if (histogramSnapshot != null && histogramSnapshot.histogramCounts().length > 0) {
                writer.writeSeparator();
                serializeHistogram(id, histogramSnapshot, writer, dedotMetricName);
            }
            return true;
        }
        return hasValue;
    }

    private static void serializeHistogram(Meter.Id id, HistogramSnapshot histogramSnapshot, ReportWriter writer, boolean dedotMetricName) {
        if (histogramSnapshot == null) {
            return;
        }
        String suffix = ".histogram";
        CountAtBucket[] bucket = histogramSnapshot.histogramCounts();
        writer.writeObjectStart(id.getName(), "values", suffix, dedotMetricName);
        writer.writeArrayStart();
        if (bucket.length > 0) {
            writer.writeFieldValue(bucket[0].bucket());
            for (int i = 1; i < bucket.length; i++) {
                writer.writeSeparator();
                writer.writeFieldValue(bucket[i].bucket());
            }
        }
        writer.writeArrayEnd();
        writer.writeSeparator();
        writer.writeFieldStart("counts");
        writer.writeArrayStart();
        // Micrometer bucket counts are cumulative: E.g. the count at bucket with upper
        // boundary X is the total number of observations smaller than X
        // including values which have already been counted for smaller buckets.
        // Elastic however expects non-cumulative bucket counts
        if (bucket.length > 0) {
            writer.writeFieldValue((long) bucket[0].count());
            double prevBucketCount = bucket[0].count();
            for (int i = 1; i < bucket.length; i++) {
                writer.writeSeparator();
                writer.writeFieldValue((long) (bucket[i].count() - prevBucketCount));
                prevBucketCount = bucket[i].count();
            }
        }
        writer.writeArrayEnd();

        writer.writeSeparator();
        writer.writeFieldStart("type");
        writer.writeFieldValue("histogram");

        writer.writeObjectEnd();
    }

    private static void serializeValue(Meter.Id id, String suffix, long value, ReportWriter writer, boolean dedotMetricName) {
        serializeValueStart(id.getName(), suffix, writer, dedotMetricName);
        writer.writeFieldValue(value);
        writer.writeObjectEnd();
    }

    /**
     * Conditionally serializes a {@code double} value if the value is valid, i.e. neither Double.NaN nor +/-Infinite
     *
     * @param id       meter ID
     * @param value    meter value
     * @param hasValue whether a value has already been written
     * @param writer       writer
     * @param dedotMetricName
     * @return true if a value has been written before, including this one; false otherwise
     */
    private static boolean serializeValue(Meter.Id id, double value, boolean hasValue, ReportWriter writer, boolean dedotMetricName) {
        if (isValidValue(value)) {
            if (hasValue) writer.writeSeparator();
            serializeValue(id, "", value, writer, dedotMetricName);
            return true;
        }
        return hasValue;
    }

    private static void serializeValue(Meter.Id id, String suffix, double value, ReportWriter writer, boolean dedotMetricName) {
        serializeValueStart(id.getName(), suffix, writer, dedotMetricName);
        writer.writeFieldValue(value);
        writer.writeObjectEnd();
    }

    private static void serializeValueStart(String key, String suffix, ReportWriter writer, boolean dedotMetricName) {
        writer.writeObjectStart(key, "value",  suffix, dedotMetricName);
    }

    private static boolean isValidValue(double value) {
        return !Double.isNaN(value) && !Double.isInfinite(value);
    }
}
