package mx.j2.recommend.component.base;

/**
 * 所有组件的基类
 */
public abstract class BaseComponent implements IComponent {
    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }
}
