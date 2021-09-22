package mx.j2.recommend.component.list.skip.base;

import mx.j2.recommend.component.list.skip.ISkip;

import java.util.ArrayList;
import java.util.List;

/**
 * 多重 skip 基类
 *
 * 可以添加多个 skip 条件，然后只在配置文件里写一个这样的 skip 即可。
 * 如果不使用此类，也可以灵活使用继承的方式来实现多重 skip。
 *
 * 优点：
 * 配置简单
 *
 * 缺点：
 * 不灵活，可读性和可重用性差
 */
public abstract class BaseMultiSkip<T> implements ISkip<T> {
    private List<ISkip<T>> skips = new ArrayList<>();

    BaseMultiSkip() {
        addSkips(skips);
    }

    @Override
    public boolean skip(T data) {
        for (ISkip<T> skipIt : skips) {
            if (skipIt.skip(data)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 收集所有需要的 skip 实例
     */
    abstract void addSkips(List<ISkip<T>> skipList);
}
