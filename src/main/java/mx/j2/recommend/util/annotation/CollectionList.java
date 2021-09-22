package mx.j2.recommend.util.annotation;

import java.lang.annotation.*;

/**
 * @author xiang.zhou
 * @description
 * @date 2020-09-03
 */

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CollectionList {
    /**
     * 拉链类型枚举
     * @author peida
     */
    public enum Type { TOPHOT, POOL, DEFAULT}

    Type type() default Type.DEFAULT;

    String suffix() default "";
}