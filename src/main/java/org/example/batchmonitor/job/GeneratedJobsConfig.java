package org.example.batchmonitor.job;

import io.micrometer.observation.ObservationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.context.annotation.Configuration;

/**
 * 대시보드 확인용으로 sampleJob01 ~ sampleJob36 을 반복문으로 등록한다.
 * - Step 수: 1~3개 (index % 3 + 1)
 * - Step당 소요 시간: 50~750ms 정도 (index 기반 고정값)
 * - index가 9의 배수이면 마지막 Step에서 실패
 */
@Configuration
public class GeneratedJobsConfig implements BeanDefinitionRegistryPostProcessor {

    private static final Logger log = LoggerFactory.getLogger(GeneratedJobsConfig.class);
    private static final int COUNT = 36;

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        for (int i = 1; i <= COUNT; i++) {
            String name = "sampleJob%02d".formatted(i);
            AbstractBeanDefinition definition = BeanDefinitionBuilder
                    .genericBeanDefinition(GeneratedJobsConfig.class)
                    .setFactoryMethod("create")
                    .addConstructorArgValue(name)
                    .addConstructorArgValue(i)
                    // 나머지 인자(JobRepository, 리스너, 레지스트리)는 타입으로 주입
                    .setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR)
                    .setScope(BeanDefinition.SCOPE_SINGLETON)
                    .getBeanDefinition();
            registry.registerBeanDefinition(name, definition);
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
    }

    public static Job create(String name, int index, JobRepository jobRepository, LastSuccessListener listener,
                             ObservationRegistry observationRegistry) {
        int steps = index % 3 + 1;
        long sleepMillis = 50 + (index * 37L) % 700;
        boolean fails = index % 9 == 0;

        SimpleJobBuilder builder = null;
        for (int s = 1; s <= steps; s++) {
            boolean last = s == steps;
            String stepName = name + ".step" + s;
            // 빈이 아닌 Step은 Boot가 관측 설정을 넣어주지 않으므로 직접 지정해야 step 메트릭이 생긴다
            var step = new StepBuilder(stepName, jobRepository)
                    .observationRegistry(observationRegistry)
                    .tasklet((contribution, chunkContext) -> {
                        Thread.sleep(sleepMillis);
                        if (last && fails) {
                            throw new IllegalStateException(stepName + " 실패 (샘플)");
                        }
                        log.info("{} 완료 ({}ms)", stepName, sleepMillis);
                        return RepeatStatus.FINISHED;
                    })
                    .build();
            builder = builder == null
                    ? new JobBuilder(name, jobRepository).listener(listener).start(step)
                    : builder.next(step);
        }
        return builder.build();
    }
}
