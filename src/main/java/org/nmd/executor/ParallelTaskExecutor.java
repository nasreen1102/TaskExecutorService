package org.nmd.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ParallelTaskExecutor<I, O> {
    public static final Logger LOG = LoggerFactory.getLogger(ParallelTaskExecutor.class);

    private final ExecutorCompletionService<O> completionService;

    public ParallelTaskExecutor(ThreadPoolTaskExecutor executor) {
        this.completionService = new ExecutorCompletionService<>(executor);
    }

    public List<Future<O>> submit(List<I> inputList, final CallableAction<I, O> action){
        return inputList.stream()
                .map(request -> completionService.submit(()-> action.action(request)))
                .collect(Collectors.toList());
    }

    public List<O> get(List<Future<O>> futureList, int timeoutInMillis) throws InterruptedException, ExecutionException,
            TimeoutException {
        List<O> response =new ArrayList<>();
        for (int i = 0; i < futureList.size(); i++) {
            try {
                response.add(completionService.take().get(
                        timeoutInMillis, TimeUnit.MILLISECONDS));
            }catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.error("Exception while reading future "+ e);
                throw e;
            }
        }
        return response;
    }

    public static ParallelTaskExecutorHandler getExecutorHandler(int corePoolSize, int maxPoolSize, int keepAliveSeconds,
                                                                                                         int queueCapacity, boolean tasksToCompleteOnShutdown){
        return new ParallelTaskExecutorHandler(corePoolSize, maxPoolSize,
                keepAliveSeconds, queueCapacity, tasksToCompleteOnShutdown);
    }
}
