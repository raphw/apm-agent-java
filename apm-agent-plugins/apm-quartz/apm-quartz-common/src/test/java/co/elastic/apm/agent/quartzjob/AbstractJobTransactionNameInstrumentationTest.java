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
package co.elastic.apm.agent.quartzjob;

import co.elastic.apm.agent.AbstractInstrumentationTest;
import co.elastic.apm.agent.impl.ElasticApmTracer;
import co.elastic.apm.agent.impl.TracerInternalApiUtils;
import co.elastic.apm.agent.impl.transaction.SpanImpl;
import co.elastic.apm.agent.impl.transaction.TransactionImpl;
import co.elastic.apm.agent.tracer.Outcome;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.quartz.JobDetail;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.impl.StdSchedulerFactory;

import static org.assertj.core.api.Assertions.assertThat;


abstract class AbstractJobTransactionNameInstrumentationTest extends AbstractInstrumentationTest {

    protected Scheduler scheduler;

    @BeforeEach
    void prepare() throws SchedulerException {
        scheduler = new StdSchedulerFactory().getScheduler();
        scheduler.start();
    }

    @AfterEach
    void cleanup() throws SchedulerException {
        scheduler.shutdown();
    }

    @Test
    void testJobWithGroup() throws SchedulerException {
        JobDetail job = buildJobDetailTestJob("dummyJobName", "group1");
        scheduler.scheduleJob(job, createTrigger());

        verifyTransactionFromJobDetails(job, Outcome.SUCCESS);
    }

    @Test
    void testJobWithoutGroup() throws SchedulerException {
        JobDetail job = buildJobDetailTestJob("dummyJobName", null);
        scheduler.scheduleJob(job, createTrigger());

        verifyTransactionFromJobDetails(job, Outcome.SUCCESS);
    }

    @Test
    void testJobManualCall() throws SchedulerException, InterruptedException {
        executeTestJobCreatingSpan(tracer, true);

        reporter.awaitTransactionCount(1);
        TransactionImpl transaction = reporter.getFirstTransaction();

        verifyTransaction(transaction, "TestJobCreatingSpan#execute");

        assertThat(reporter.getNumReportedSpans()).isEqualTo(1);
        SpanImpl span = reporter.getFirstSpan();
        assertThat(span.getTraceContext().getParentId()).isEqualTo(transaction.getTraceContext().getId());
    }

    @Test
    public void testAgentPaused() throws SchedulerException {
        TracerInternalApiUtils.pauseTracer(tracer);
        int transactionCount = objectPoolFactory.getTransactionPool().getRequestedObjectCount();
        int spanCount = objectPoolFactory.getSpanPool().getRequestedObjectCount();

        executeTestJobCreatingSpan(tracer, false);

        assertThat(reporter.getTransactions()).isEmpty();
        assertThat(reporter.getSpans()).isEmpty();
        assertThat(objectPoolFactory.getTransactionPool().getRequestedObjectCount()).isEqualTo(transactionCount);
        assertThat(objectPoolFactory.getSpanPool().getRequestedObjectCount()).isEqualTo(spanCount);
    }



    @Test
    void testJobWithResult() throws SchedulerException {
        JobDetail job = buildJobDetailTestJobWithResult("dummyJobName");
        scheduler.scheduleJob(job, createTrigger());

        TransactionImpl transaction = verifyTransactionFromJobDetails(job, Outcome.SUCCESS);
        assertThat(transaction.getResult()).isEqualTo("this is the result");
    }

    @Test
    void testJobWithException() throws SchedulerException {
        JobDetail job = buildJobDetailTestJobWithException("dummyJobName");
        scheduler.scheduleJob(job, createTrigger());

        verifyTransactionFromJobDetails(job, Outcome.FAILURE);
    }

    abstract TransactionImpl verifyTransactionFromJobDetails(JobDetail job, Outcome expectedOutcome);

    public TransactionImpl verifyTransaction(TransactionImpl transaction, String expectedName) {
        assertThat(transaction.getType()).isEqualToIgnoringCase("scheduled");
        assertThat(transaction.getNameAsString())
            .isEqualTo(expectedName);
        assertThat(transaction.getFrameworkName()).isEqualTo("Quartz");
        assertThat(transaction.getFrameworkVersion()).isEqualTo(quartzVersion());

        return transaction;
    }

    abstract SimpleTrigger createTrigger();

    abstract String quartzVersion();

    abstract JobDetail buildJobDetailTestJob(String name, String groupName);

    abstract void executeTestJobCreatingSpan(ElasticApmTracer tracer, boolean traced) throws JobExecutionException;

    abstract JobDetail buildJobDetailTestJobWithResult(String name);

    abstract JobDetail buildJobDetailTestJobWithException(String name);

}
