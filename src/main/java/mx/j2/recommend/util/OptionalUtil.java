package mx.j2.recommend.util;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author ：zhongrenli
 * @date ：Created in 8:42 下午 2020/10/15
 * @desc ：链式调用 Bean 中的 value 方法
 */
public class OptionalUtil<T>{
    private static final OptionalUtil<?> EMPTY = new OptionalUtil<>();

    private final T value;

    private OptionalUtil() {
        this.value = null;
    }

    /**
     * 空值会抛出空指针
     * @param value
     */
    private OptionalUtil(T value) {
        this.value = Objects.requireNonNull(value);
    }

    /**
     * 包装一个不能为空的 bean
     * @param value
     * @param <T>
     * @return
     */
    public static <T> OptionalUtil<T> of (T value) {
        return new OptionalUtil<>(value);
    }

    /**
     * 包装一个可能为空的 bean
     * @param value
     * @param <T>
     * @return
     */
    public static <T> OptionalUtil<T> ofNullable(T value) {
        return value == null ? empty() : of(value);
    }

    /**
     * 取出具体的值
     * @return
     */
    public T get() {
        return Objects.isNull(value) ? null : value;
    }

    /**
     * 取出一个可能为空的对象
     * @param fn
     * @param <R>
     * @return
     */
    public <R> OptionalUtil<R> getUtil(Function<? super T, ? extends R> fn) {
        return Objects.isNull(value) ? OptionalUtil.empty() : OptionalUtil.ofNullable(fn.apply(value));
    }

    /**
     * 如果目标为空，使用给定的值
     * @param other
     * @return
     */
    public T orElse(T other) {
        return value != null ? value : other;
    }

    /**
     * 如果目标值为空，通过 lambda 表达式获取一个值
     * @param other
     * @return
     */
    public T orElseGet( Supplier<? extends T> other) {
        return value != null ? value : other.get();
    }

    /**
     * 如果目标值为空，抛出一个异常
     * @param exceptionSupplier
     * @param <X>
     * @return
     * @throws X
     */
    public <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X{
        if (value != null) {
            return value;
        } else {
            throw exceptionSupplier.get();
        }
    }

    public boolean isPresent() {
        return value != null;
    }

    public void ifPresent(Consumer<? super T> consumer) {
        if (value != null) {
            consumer.accept(value);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    /**
     * 空值常量
     * @param <T>
     * @return
     */
    public static <T> OptionalUtil<T> empty() {
        @SuppressWarnings("unchecked")
        OptionalUtil<T> none = (OptionalUtil<T>) EMPTY;

        return none;
    }
}
