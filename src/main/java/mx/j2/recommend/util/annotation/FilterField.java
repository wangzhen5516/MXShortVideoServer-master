package mx.j2.recommend.util.annotation;

import java.lang.annotation.*;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:37 下午 2021/3/2
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface FilterField {
    String name() default "";
}
