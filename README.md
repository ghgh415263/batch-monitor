# 스프링 배치 핵심 기본 정리

Oct 17, 2026 · @개발자

## 1. 스프링 배치란

스프링 배치는 대량의 데이터를 **읽고 → 가공하고 → 쓰는** 작업을 안정적으로 반복 실행하게 해 주는 프레임워크입니다. 핵심 가치는 "실패해도 이어서 다시 할 수 있다"는 점입니다.

언제 쓰나요?

- 매일 밤 정산, 통계 집계, 대량 메일 발송
- 파일(CSV, JSON) ↔ DB 대량 이관
- 수십만\~수천만 건을 나눠서 커밋해야 하는 작업

스프링 배치가 대신 해 주는 것: 트랜잭션 단위 커밋(청크), 실행 이력 저장, 실패 지점부터 재시작, skip/retry, 진행 상황 기록.

## 2. 핵심 구성요소

**Job은 Step들의 묶음이고, 실행할 때마다 그 결과가 JobRepository에 기록됩니다.** 이 기록 덕분에 실패한 지점부터 재시작할 수 있습니다.

&#91;embedded content: 스프링 배치 구성요소 · Job, Step, JobRepository\]

| 용어 | 한 줄 설명 | 예시 |
| --- | --- | --- |
| Job | 배치 작업 전체. 하나 이상의 Step으로 구성 | 일일 정산 Job |
| Step | Job의 한 단계. Tasklet 또는 Chunk 방식 | 파일 읽어서 DB 저장 |
| JobInstance | Job 이름 + 식별 파라미터 조합 하나 | 정산 Job (date=2026-10-17) |
| JobExecution | JobInstance의 실행 시도 1회. 실패 후 재실행하면 Execution만 늘어남 | 1차 실패, 2차 성공 |
| StepExecution | Step 실행 시도 1회. 읽은/쓴/스킵 건수 기록 | read=1000, write=998 |
| ExecutionContext | 재시작용 상태 저장소 | "몇 번째 줄까지 읽었는지" |
| JobRepository | 배치 이력을 저장하는 저장소 | BATCH\_JOB\_EXECUTION 등 테이블 |
| JobOperator | Job 실행·중지·재시작. 6.0에서 JobLauncher를 대체 | `jobOperator.start(job, params)` |

## 3. Chunk 모델 — 스프링 배치의 심장

**청크(Chunk)는 "N건을 모아서 한 번에 쓰고 커밋하는" 단위입니다.** 100만 건을 한 트랜잭션으로 처리하면 중간에 실패했을 때 전부 롤백되지만, 100건씩 나누면 실패한 청크만 다시 하면 됩니다.

&#91;embedded content: Chunk 처리 흐름 · 읽기 → 가공 → 쓰기 → 커밋\]

Reader와 Processor는 **1건씩**, Writer는 \*\*청크 단위(List)\*\*로 동작합니다. Reader가 `null`을 반환하면 "더 이상 데이터 없음"으로 보고 Step을 끝냅니다.

| 역할 | 인터페이스 | 자주 쓰는 구현체 |
| --- | --- | --- |
| 읽기 | `ItemReader<T>` | `FlatFileItemReader`(CSV), `JdbcPagingItemReader`, `JdbcCursorItemReader`, `JpaPagingItemReader` |
| 가공 | `ItemProcessor<I, O>` | 람다로 직접 작성. `null` 반환 시 해당 건 제외 |
| 쓰기 | `ItemWriter<T>` | `JdbcBatchItemWriter`, `JpaItemWriter`, `FlatFileItemWriter` |

Tasklet과 비교하면: 대량 데이터를 읽고 쓰는 일은 Chunk, 한 번 실행하고 끝나는 일은 Tasklet입니다.

## 4. 예제 1 — Tasklet으로 만드는 가장 단순한 Job

Tasklet은 "한 번 실행하고 끝나는" 단일 작업입니다. 파일 삭제, 테이블 초기화, 알림 발송처럼 읽기/쓰기 반복이 필요 없는 일에 씁니다.

```java
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HelloJobConfig {

    @Bean
    public Job helloJob(JobRepository jobRepository, Step helloStep) {
        return new JobBuilder("helloJob", jobRepository)
                .start(helloStep)              // 첫 Step
                .build();
    }

    @Bean
    public Step helloStep(JobRepository jobRepository) {
        return new StepBuilder("helloStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Hello, Spring Batch!");
                    return RepeatStatus.FINISHED;   // 끝. CONTINUABLE이면 다시 호출됨
                })
                .build();
    }
}
```

포인트

- `JobBuilder`, `StepBuilder` 모두 이름 + `JobRepository`를 받습니다. 이름이 이력 테이블의 키가 됩니다.
- `RepeatStatus.FINISHED`를 반환해야 Step이 끝납니다.

## 5. 예제 2 — Chunk로 CSV 읽어서 DB에 저장하기

CSV의 사람 목록을 읽어 이름을 대문자로 바꾼 뒤 `people` 테이블에 100건씩 커밋합니다. 실무 배치의 90%가 이 모양입니다.

**입력 파일** `src/main/resources/people.csv`

```csv
firstName,lastName
jill,doe
joe,doe
justin,doe
```

**대상 테이블** `src/main/resources/schema.sql`

```sql
CREATE TABLE people (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name  VARCHAR(50)
);
```

**도메인**

```java
public record Person(String firstName, String lastName) {}
```

**Job 설정**

```java
import javax.sql.DataSource;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class CsvToDbJobConfig {

    // 1) Reader: CSV 한 줄 → Person 한 개
    @Bean
    public FlatFileItemReader<Person> personReader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personReader")                 // 재시작 시 위치 저장용 키
                .resource(new ClassPathResource("people.csv"))
                .linesToSkip(1)                       // 헤더 건너뛰기
                .delimited()
                .names("firstName", "lastName")
                .targetType(Person.class)
                .build();
    }

    // 2) Processor: 가공 (null을 반환하면 그 아이템은 버려짐 = 필터)
    @Bean
    public ItemProcessor<Person, Person> personProcessor() {
        return person -> new Person(
                person.firstName().toUpperCase(),
                person.lastName().toUpperCase());
    }

    // 3) Writer: 청크(List) 단위로 한 번에 INSERT
    @Bean
    public JdbcBatchItemWriter<Person> personWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .dataSource(dataSource)
                .sql("INSERT INTO people (first_name, last_name) VALUES (?, ?)")
                .itemPreparedStatementSetter((person, ps) -> {
                    ps.setString(1, person.firstName());
                    ps.setString(2, person.lastName());
                })
                .build();
    }

    // 4) Step: 100건 단위로 읽기→가공→쓰기→커밋 반복
    @Bean
    public Step csvToDbStep(JobRepository jobRepository,
                            PlatformTransactionManager transactionManager,
                            FlatFileItemReader<Person> personReader,
                            ItemProcessor<Person, Person> personProcessor,
                            JdbcBatchItemWriter<Person> personWriter) {
        return new StepBuilder("csvToDbStep", jobRepository)
                .<Person, Person>chunk(100)           // 커밋 단위 (chunk size)
                .transactionManager(transactionManager)
                .reader(personReader)
                .processor(personProcessor)
                .writer(personWriter)
                .build();
    }

    // 5) Job: Step을 묶는 실행 단위
    @Bean
    public Job csvToDbJob(JobRepository jobRepository, Step csvToDbStep) {
        return new JobBuilder("csvToDbJob", jobRepository)
                .incrementer(new RunIdIncrementer())  // 매 실행마다 새 JobInstance
                .start(csvToDbStep)
                .build();
    }
}
```

포인트

- 제네릭 `<입력, 출력>`이 Reader → Processor → Writer 타입을 맞춰 줍니다.
- Processor는 생략 가능합니다. 생략하면 `<Person, Person>`처럼 입력=출력이어야 합니다.
- 5.x의 `.chunk(100, transactionManager)`는 6.0에서 deprecated입니다. `.chunk(100).transactionManager(tm)`로 씁니다.

## 6. JobParameters와 실행 방법

JobParameters는 실행할 때 밖에서 넣는 값(날짜, 파일 경로 등)입니다. **Job 이름 + 식별 파라미터가 같으면 같은 JobInstance**로 보기 때문에, 이미 성공한 조합은 다시 실행되지 않습니다.

**파라미터를 Bean에서 받기 — `@StepScope`**

```java
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;

@Bean
@StepScope   // Step 실행 시점에 Bean 생성 → jobParameters 주입 가능
public FlatFileItemReader<Person> personReader(
        @Value("#{jobParameters['inputFile']}") String inputFile) {
    return new FlatFileItemReaderBuilder<Person>()
            .name("personReader")
            .resource(new FileSystemResource(inputFile))
            .linesToSkip(1)
            .delimited()
            .names("firstName", "lastName")
            .targetType(Person.class)
            .build();
}
```

**실행 방법 A — Spring Boot 기동 시 자동 실행**

```bash
java -jar batch-app.jar inputFile=/data/people.csv targetDate=2026-10-17
```

`key=value` 형태의 인자가 JobParameters로 들어갑니다.

**실행 방법 B — 코드에서 직접 실행 (`JobOperator`)**

```java
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;

@RestController
@RequiredArgsConstructor
public class BatchController {

    private final JobOperator jobOperator;
    private final Job csvToDbJob;

    @PostMapping("/batch/run")
    public String run(@RequestParam String inputFile) throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addString("inputFile", inputFile)
                .addLong("time", System.currentTimeMillis())   // 매번 새 실행이 되도록
                .toJobParameters();

        var execution = jobOperator.start(csvToDbJob, params);
        return execution.getStatus().toString();
    }
}
```

이 방식을 쓸 때는 `spring.batch.job.enabled: false`로 기동 시 자동 실행을 끕니다.

주의 (6.0 변경점)

- 5.x의 `JobLauncher`는 deprecated입니다. `JobOperator`가 `JobLauncher`를 대체합니다.
- Job에 `RunIdIncrementer` 같은 incrementer를 붙이면 6.0부터는 **사용자가 넘긴 파라미터가 무시**됩니다(경고 로그만 남음). 외부 파라미터를 받는 Job에는 incrementer를 붙이지 마세요.

## 7. 자주 쓰는 추가 기능

기본기 다음으로 가장 많이 쓰는 세 가지만 추렸습니다.

**① Skip / Retry — 일부 실패해도 계속 가기**** ****(****c****h****unk**** ****기****반 ****step****에****만 ****사****용가****능****)**

- Skip: 잘못된 데이터 몇 건은 건너뛰고 진행
- Retry: 일시적 오류(네트워크, 락)는 몇 번 다시 시도

```java
import java.util.Set;
import org.springframework.batch.core.step.skip.LimitCheckingExceptionHierarchySkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.dao.TransientDataAccessException;

@Bean
public Step faultTolerantStep(JobRepository jobRepository,
                              PlatformTransactionManager transactionManager,
                              FlatFileItemReader<Person> reader,
                              JdbcBatchItemWriter<Person> writer) {

    RetryPolicy retryPolicy = RetryPolicy.builder()
            .maxRetries(3)
            .includes(Set.of(TransientDataAccessException.class))
            .build();

    SkipPolicy skipPolicy = new LimitCheckingExceptionHierarchySkipPolicy(
            Set.of(FlatFileParseException.class), 10);   // 파싱 오류 최대 10건까지 skip

    return new StepBuilder("faultTolerantStep", jobRepository)
            .<Person, Person>chunk(100)
            .transactionManager(transactionManager)
            .reader(reader)
            .writer(writer)
            .faultTolerant()
            .retryPolicy(retryPolicy)
            .skipPolicy(skipPolicy)
            .build();
}
```

6.0부터 retry는 Spring Retry 라이브러리 대신 Spring Framework 7의 `RetryPolicy`를 쓰고, skip은 `SkipPolicy` 하나로 설정합니다.

**② Listener — 시작/종료 시점에 끼워 넣기**

```java
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;

@Component
public class JobLoggingListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("시작: " + jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("종료 상태: " + jobExecution.getStatus());   // COMPLETED, FAILED ...
    }
}

// 등록
new JobBuilder("csvToDbJob", jobRepository)
        .listener(jobLoggingListener)
        .start(csvToDbStep)
        .build();
```

Step 단위는 `StepExecutionListener`, 청크 단위는 `ChunkListener`, 아이템 단위는 `ItemReadListener` / `ItemWriteListener` / `SkipListener`가 있습니다.

**③ 흐름 제어 — 여러 Step 순서와 분기**

```
// 순차 실행: A → B → C (중간에 실패하면 Job 실패)
new JobBuilder("seqJob", jobRepository)
        .start(stepA)
        .next(stepB)
        .next(stepC)
        .build();

// 조건 분기: A가 실패하면 알림 Step, 그 외에는 B
new JobBuilder("flowJob", jobRepository)
        .start(stepA)
            .on("FAILED").to(notifyStep)
        .from(stepA)
            .on("*").to(stepB)
        .end()
        .build();
```

## 출처

- [What's new in Spring Batch 6 (공식 문서)](https://docs.spring.io/spring-batch/reference/whatsnew.html)
- [Spring Batch 6.0 Migration Guide](https://github.com/spring-projects/spring-batch/wiki/Spring-Batch-6.0-Migration-Guide)
- [Spring Boot 4 — Spring Batch](https://docs.spring.io/spring-boot/4.0/reference/io/spring-batch.html)
