package org.example.batchmonitor.job;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.support.ListItemReader;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SimpleJobsConfig {

    private static final Logger log = LoggerFactory.getLogger(SimpleJobsConfig.class);

    // 1. 로그만 찍는 Job
    @Bean
    public Job helloJob(JobRepository jobRepository, Step helloStep, LastSuccessListener listener) {
        return new JobBuilder("helloJob", jobRepository)
                .listener(listener)
                .start(helloStep)
                .build();
    }

    @Bean
    public Step helloStep(JobRepository jobRepository) {
        return new StepBuilder("helloStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    log.info("Hello, Spring Batch!");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    // 2. 두 Step을 순서대로 실행하는 Job
    @Bean
    public Job twoStepJob(JobRepository jobRepository, Step prepareStep, Step processStep,
                          LastSuccessListener listener) {
        return new JobBuilder("twoStepJob", jobRepository)
                .listener(listener)
                .start(prepareStep)
                .next(processStep)
                .build();
    }

    @Bean
    public Step prepareStep(JobRepository jobRepository) {
        return new StepBuilder("prepareStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    log.info("준비 단계");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Step processStep(JobRepository jobRepository) {
        return new StepBuilder("processStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    log.info("처리 단계");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    // 3. 항상 실패하는 Job (모니터링 대시보드의 실패 상태 확인용)
    @Bean
    public Job failJob(JobRepository jobRepository, Step failStep, LastSuccessListener listener) {
        return new JobBuilder("failJob", jobRepository)
                .listener(listener)
                .start(failStep)
                .build();
    }

    @Bean
    public Step failStep(JobRepository jobRepository) {
        return new StepBuilder("failStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    throw new IllegalStateException("의도된 실패입니다");
                })
                .build();
    }

    // 4. 청크 방식 Job: 숫자 1~10을 읽어 제곱 후 로그 출력
    @Bean
    public Job squareJob(JobRepository jobRepository, Step squareStep, LastSuccessListener listener) {
        return new JobBuilder("squareJob", jobRepository)
                .listener(listener)
                .start(squareStep)
                .build();
    }

    @Bean
    public Step squareStep(JobRepository jobRepository) {
        return new StepBuilder("squareStep", jobRepository)
                .<Integer, Integer>chunk(3)
                .reader(new ListItemReader<>(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
                .processor(n -> n * n)
                .writer(items -> log.info("제곱 결과: {}", items.getItems()))
                .build();
    }
}
