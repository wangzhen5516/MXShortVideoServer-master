package mx.j2.recommend.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author xiang.zhou
 * @description 分批处理工具
 * @date 2020-07-07
 */

public class BatchUtil {

    public static <T> void partitionRun(List<T> dataList, int size, Consumer<List<T>> consumer) {
        if (MXJudgeUtils.isEmpty(dataList)) {
            return;
        }
        Preconditions.checkArgument(size > 0, "size must not be a minus");
        Lists.partition(dataList, size).forEach(consumer);
    }

    public static <T> List<T> partitionCall2List(List<T> dataList, int size, Function<List<T>, List<T>> function) {

        if (MXJudgeUtils.isEmpty(dataList)) {
            return new ArrayList<>(0);
        }
        Preconditions.checkArgument(size > 0, "size must not be a minus");

        return Lists.partition(dataList, size).stream().map(function).filter(Objects::nonNull).reduce(new ArrayList<>(), (resultList1, resultList2) -> {
            resultList1.addAll(resultList2);
            return resultList1;
        });
    }

    public static <T, V> List<V> partitionCall2ListAsync(List<T> dataList,
                                                         int size,
                                                         ExecutorService executorService,
                                                         Function<List<T>, List<V>> function) {
        if (MXJudgeUtils.isEmpty(dataList)) {
            return new ArrayList<>(0);
        }
        Preconditions.checkArgument(size > 0, "size must not be a minus");

        List<CompletableFuture<List<V>>> completableFutures = Lists.partition(dataList, size)
                .stream()
                .map(eachList -> {
                    if (executorService == null) {
                        return CompletableFuture.supplyAsync(() -> function.apply(eachList));
                    } else {
                        return CompletableFuture.supplyAsync(() -> function.apply(eachList), executorService);
                    }
                })
                .collect(Collectors.toList());

        CompletableFuture<Void> allFinished = CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]));
        try {
            allFinished.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return completableFutures.stream()
                .map(CompletableFuture::join)
                .filter(MXJudgeUtils::isNotEmpty)
                .reduce(new ArrayList<>(), ((list1, list2) -> {
                    List<V> resultList = new ArrayList<>();
                    if(MXJudgeUtils.isNotEmpty(list1)){
                        resultList.addAll(list1);
                    }
                    if(MXJudgeUtils.isNotEmpty(list2)){
                        resultList.addAll(list2);
                    }
                    return resultList;
                }));
    }

    public static <T, V> Map<T, V> partitionCall2Map(List<T> dataList, int size, Function<List<T>, Map<T, V>> function) {
        if (MXJudgeUtils.isEmpty(dataList)) {
            return new HashMap<>(0);
        }
        Preconditions.checkArgument(size > 0, "size must not be a minus");
        return Lists.partition(dataList, size).stream().map(function).filter(Objects::nonNull).reduce(new HashMap<>(), (resultMap1, resultMap2) -> {
            resultMap1.putAll(resultMap2);
            return resultMap1;
        });
    }
}