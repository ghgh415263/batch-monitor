package org.example.batchmonitor.job;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.stereotype.Component;

/**
 * Job 종료 시 마지막 실행 상태와 성공/실패 시각을 게이지로 등록한다. (프로세스당 Job 1개 실행 전제)
 * - batch_job_last_status{status="COMPLETED|FAILED|STOPPED|..."}: 값은 항상 1, 상태는 BatchStatus 라벨
 * - batch_job_last_success_timestamp_seconds: COMPLETED일 때
 * - batch_job_last_failure_timestamp_seconds: 그 외(FAILED, STOPPED 등)일 때
 *   해당 결과일 때만 등록하므로, Pushgateway(POST)에는 반대편의 이전 값이 그대로 남는다.
 */
@Component
public class LastSuccessListener implements JobExecutionListener {

    private static final String JOB_NAME_TAG = "spring.batch.job.name";

    private final MeterRegistry registry;
    // Gauge는 값을 약한 참조로 읽으므로 푸시 전에 GC되지 않게 필드로 쥐고 있는다
    private final AtomicLong one = new AtomicLong(1);
    private final AtomicLong timestamp = new AtomicLong();

    public LastSuccessListener(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        BatchStatus batchStatus = jobExecution.getStatus();
        boolean success = batchStatus == BatchStatus.COMPLETED;

        timestamp.set(Instant.now().getEpochSecond());

        // 마지막 상태는 라벨로 표현한다. 값은 항상 1이고, Pushgateway가 패밀리를 통째로 교체하므로 최신 상태 하나만 남는다
        Gauge.builder("batch.job.last.status", one, AtomicLong::get)
                .tag(JOB_NAME_TAG, jobName)
                .tag("status", batchStatus.name())
                .register(registry);
        Gauge.builder(success ? "batch.job.last.success.timestamp" : "batch.job.last.failure.timestamp",
                        timestamp, AtomicLong::get)
                .tag(JOB_NAME_TAG, jobName)
                .baseUnit("seconds")
                .register(registry);
    }
}
