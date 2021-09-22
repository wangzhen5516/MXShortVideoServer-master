package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.base.BaseComponent;
import mx.j2.recommend.component.base.IComponent;
import mx.j2.recommend.manager.IComponentManager;
import mx.j2.recommend.util.ClassUtil;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/4/19 下午5:52
 * @description 组件管理器基类
 */
public abstract class BaseComponentManager<T extends BaseComponent> extends BaseManager
        implements IComponentManager<T> {

    /**
     * 组件集合
     */
    HashMap<String, T> componentMap = new HashMap<>();

    BaseComponentManager() {
        initComponents();
    }

    /**
     * 实例化组件
     */
    private void initComponents() {
        try {
            List<Class> classes = ClassUtil.getAllClassByInterface(Class.forName(getComponentInterfacePath()));

            for (Class c : classes) {
                if (!Modifier.isAbstract(c.getModifiers())) {
                    T componentIt = (T) c.getConstructor().newInstance();
                    componentMap.put(componentIt.getName(), componentIt);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public T getComponentInstance(String name) {
        return componentMap.get(name);
    }

    /**
     * 返回组件接口路径
     */
    @Override
    public String getComponentInterfacePath() {
        return getComponentType().getComponentIfPath();
    }

    /**
     * 获取组件类型
     */
    abstract IComponent.TypeEnum getComponentType();
}
