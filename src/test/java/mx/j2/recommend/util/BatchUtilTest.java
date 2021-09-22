package mx.j2.recommend.util;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author xiang.zhou
 * @description
 * @date 2020-07-07
 */
public class BatchUtilTest {

    // 测试数据
    private List<String> mockDataList = new ArrayList<>();

    private final int total = 100;

    private AtomicInteger atomicInteger;

    List<String> toDel = Collections.synchronizedList(new ArrayList<>());

    @Before
    public void init() {
        mockDataList.clear();
        // 构造total条数据
        for(int i = 0;i<total;i++) {
            String s = "";
            for(int j = 0;j<i;j++) {
                s += i;
            }
            mockDataList.add(s);
        }
    }

    @Test
    public void test3() {
        List<String> dataList = new ArrayList<>();
        dataList.add("1");
        dataList.add("2");
        dataList.add("3");
        List l = Lists.partition(dataList, 2);
        System.out.println(l.size());
    }

    @Test
    public void test_call_return_list_partition_async() {
        List<String> myMockList = new ArrayList<>();
        myMockList.clear();
        // 构造total条数据
        for(int i = 0;i<total;i++) {
            String s = "";
            for(int j = 0;j<i;j++) {
                s += i;
            }
            myMockList.add(s);
        }
        Assert.assertEquals(total, mockDataList.size());
        atomicInteger = new AtomicInteger(0);
        Stopwatch stopwatch = Stopwatch.createStarted();
        // 分批执行
        int size = 2;
        List<Integer> resultList = BatchUtil.partitionCall2ListAsync(
                myMockList,
                size, null,
                (eachList) -> someCall(2L, eachList)
        );

        Stopwatch stop = stopwatch.stop();

        Assert.assertEquals(total, resultList.size());
        // 正好几轮
        int turns;
        if (total % size == 0) {
            turns = total / size;
        } else {
            turns = total / size + 1;
        }
        Assert.assertEquals(turns, atomicInteger.get());

        //顺序也一致
        for(int i =0; i< myMockList.size();i++){
            Assert.assertEquals((Integer) myMockList.get(i).length(), resultList.get(i));
        }
    }

    @Test
    public void test_press() {
        for (int i=0; i< 1000;i++) {
            new Thread(this::test_call_return_list_partition_async).start();
        }
    }

    @Test
    public void test_stream() {
        List<String> itemList = new ArrayList<String>();
        itemList.add("z");
        itemList.add("x");
        itemList.add("e");
        List<String> idsList = new ArrayList<String>();
        itemList.forEach(idsList::add);
        System.out.println(idsList.size());
    }

    @Test
    public void test_del() {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        atomicInteger = new AtomicInteger(0);
        Stopwatch stopwatch = Stopwatch.createStarted();
        // 分批执行
        int size = 2;
        List<Integer> resultList = BatchUtil.partitionCall2ListAsync(
                mockDataList,
                size, executorService, this::someDel
        );
        System.out.println(toDel);
    }

    private List<Integer> someDel(List<String> innn) {
        //TODO can't remove the innn
        for( int i=0; i<innn.size(); i++) {
            toDel.add(innn.get(i));
        }
        //TODO need to check the RETURN must not be NULL;
        return new ArrayList<>();
    }


    /**
     * 模拟一次调用
     */
    private List<Integer> someCall(Long id, List<String> strList) {
        System.out.println(strList);
        atomicInteger.incrementAndGet();
        try {
            TimeUnit.SECONDS.sleep(2L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return strList.stream()
                .map(String::length)
                .collect(Collectors.toList());
    }
}
