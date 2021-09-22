package mx.j2.recommend.util;

import mx.j2.recommend.thrift.InternalUse;
import mx.j2.recommend.thrift.Result;

/**
 * @author ：zhongrenli
 * @date ：Created in 9:18 下午 2020/10/15
 */
public class OptionalUtilTest {

    public static void main(String[] args) {
        Result result = new Result();

        String value1 = OptionalUtil.ofNullable(result)
                .getUtil(Result::getInternalUse)
                .getUtil(InternalUse::getPublisherId).get();

        System.out.println(value1);

        boolean present = OptionalUtil.ofNullable(result)
                .getUtil(Result::getInternalUse)
                .getUtil(InternalUse::getPublisherId).isPresent();

        System.out.println(present);

        OptionalUtil.ofNullable(result)
                .getUtil(Result::getInternalUse)
                .getUtil(InternalUse::getAppName)
                .ifPresent(appName -> System.out.println(String.format("appName:%s", appName)));

        String value2 = OptionalUtil.ofNullable(result)
                .getUtil(Result::getInternalUse)
                .getUtil(InternalUse::getPublisherId).orElse("publisher-test");
        System.out.println(value2);

        try {
            String value3 = OptionalUtil.ofNullable(result)
                    .getUtil(Result::getInternalUse)
                    .getUtil(InternalUse::getPublisherId).orElseThrow(() -> new RuntimeException("空指针了"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
