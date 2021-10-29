package org.nmd.executor;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class ParallelTaskExecutorHandler {
    private int corePoolSize;
    private int maxPoolSize;
    private int keepAliveSeconds;
    private int queueCapacity;
    private boolean tasksToCompleteOnShutdown;
    private ThreadPoolTaskExecutor executor;

    public ParallelTaskExecutorHandler(int corePoolSize, int maxPoolSize, int keepAliveSeconds, int queueCapacity, boolean tasksToCompleteOnShutdown) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveSeconds = keepAliveSeconds;
        this.queueCapacity = queueCapacity;
        this.tasksToCompleteOnShutdown = tasksToCompleteOnShutdown;
        init();
    }

    private void init() {
        executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(this.corePoolSize);
        executor.setMaxPoolSize(this.maxPoolSize);
        executor.setKeepAliveSeconds(keepAliveSeconds);
        executor.setQueueCapacity(queueCapacity);
        executor.setWaitForTasksToCompleteOnShutdown(tasksToCompleteOnShutdown);
        executor.initialize();
    }

    public <I, O> List<O> compute(List<I> inputList, CallableAction<I, O> callableMethod,
                                  int timeoutInMillis) throws InterruptedException, ExecutionException, TimeoutException {
        ParallelTaskExecutor<I, O> parallelExecutor = build();
        final List<Future<O>> futureList = parallelExecutor.submit(inputList, callableMethod);
        return parallelExecutor.get(futureList, timeoutInMillis);
    }

    private <I, O> ParallelTaskExecutor<I, O> build() {
        return new ParallelTaskExecutor<>(this.executor);
    }

}
