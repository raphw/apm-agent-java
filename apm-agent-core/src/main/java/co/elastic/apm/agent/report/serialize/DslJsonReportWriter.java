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
package co.elastic.apm.agent.report.serialize;

import co.elastic.apm.agent.report.Reporter;
import co.elastic.apm.agent.tracer.reporting.ReportWriter;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.NumberConverter;

import javax.annotation.Nullable;

public class DslJsonReportWriter implements ReportWriter {

    private static final byte NEW_LINE = (byte) '\n';

    private final JsonWriter jw;

    private final Reporter reporter;

    private final StringBuilder replaceBuilder = new StringBuilder();

    public DslJsonReportWriter(JsonWriter jw, Reporter reporter) {
        this.jw = jw;
        this.reporter = reporter;
    }

    @Override
    public void writeObjectStart() {
        jw.writeByte(JsonWriter.OBJECT_START);
    }

    @Override
    public void writeObjectStart(String key, String value, @Nullable String suffix, boolean dedotMetricName) {
        replaceBuilder.setLength(0);
        if (dedotMetricName) {
            DslJsonSerializer.sanitizePropertyName(key, replaceBuilder);
        } else {
            replaceBuilder.append(key);
        }
        if (suffix != null) {
            if (replaceBuilder.length() == 0) {
                replaceBuilder.append(key);
            }
            replaceBuilder.append(suffix);
        }
        jw.writeString(replaceBuilder);

        jw.writeByte(JsonWriter.SEMI);
        jw.writeByte(JsonWriter.OBJECT_START);
        jw.writeByte(JsonWriter.QUOTE);
        jw.writeAscii(value);
        jw.writeByte(JsonWriter.QUOTE);
        jw.writeByte(JsonWriter.SEMI);
    }

    @Override
    public void writeObjectEnd() {
        jw.writeByte(JsonWriter.OBJECT_END);
    }

    @Override
    public void writeArrayStart() {
        jw.writeByte(JsonWriter.ARRAY_START);
    }

    @Override
    public void writeArrayEnd() {
        jw.writeByte(JsonWriter.ARRAY_END);
    }

    @Override
    public void writeFieldStart(String name) {
        DslJsonSerializer.writeFieldName(name, jw);
    }

    @Override
    public void writeFieldStartSanitized(String value) {
        jw.writeByte(JsonWriter.QUOTE); // TODO: Should this not entail quotes?
        DslJsonSerializer.writeStringValue(DslJsonSerializer.sanitizePropertyName(value, replaceBuilder), replaceBuilder, jw);
        jw.writeByte(JsonWriter.QUOTE);
        jw.writeByte(JsonWriter.SEMI);
    }

    @Override
    public void writeFieldValue(long value) {
        NumberConverter.serialize(value, jw);
    }

    @Override
    public void writeFieldValue(double value) {
        NumberConverter.serialize(value, jw);
    }

    @Override
    public void writeFieldValue(String value) {
        DslJsonSerializer.writeStringValue(value, replaceBuilder, jw);
    }

    @Override
    public void writeSeparator() {
        jw.writeByte(JsonWriter.COMMA);
    }

    @Override
    public void writeNewLine() {
        jw.writeByte(NEW_LINE);
    }

    @Override
    public void report() {
        reporter.reportMetrics(jw);
    }

    @Override
    public int size() {
        return jw.size();
    }
}
