package org.nmd.client;

import org.nmd.executor.CallableAction;
import org.nmd.executor.ParallelTaskExecutor;
import org.nmd.executor.ParallelTaskExecutorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaskExecutorClient {
    public static final Logger LOG = LoggerFactory.getLogger(TaskExecutorClient.class);

    public static void main(String[] args) {
        final List<Integer> inputList = IntStream.range(0, 10000).boxed().collect(Collectors.toList());

        long start = System.nanoTime();
        parallelCall(inputList);
        long finish = System.nanoTime();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed for parallel call - "+timeElapsed);

        start = System.nanoTime();
        sequentialCall(inputList);
        finish = System.nanoTime();
        timeElapsed = finish - start;
        System.out.println("Time elapsed for sequential call - "+timeElapsed);
    }

    private static void sequentialCall(List<Integer> inputList) {
        final List<String> response = new ArrayList<>();
        for (Integer integer : inputList) {
            String convert = convert(integer);
            response.add(convert);
        }
        System.out.println(response);
    }

    private static void parallelCall(List<Integer> inputList) {
        CallableAction<Integer, String> intToStringConversion = TaskExecutorClient::convert;
        final ParallelTaskExecutorHandler parallelTaskExecutorHandler = ParallelTaskExecutor
                .getExecutorHandler(10, 100, 60, 10000, true);

        try {
            final List<String> computedResponse = parallelTaskExecutorHandler.compute(inputList, intToStringConversion, 500);
            System.out.println(computedResponse);
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Exception occurred while calling parallel executor -" + (e instanceof ExecutionException ? e.getCause() : e));
        } catch (TimeoutException e) {
            LOG.error("Timeout while calculating region calc response");
        }
    }

    private static String convert(Integer i) {
        return i + "";
    }
}
