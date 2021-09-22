package mx.j2.recommend.manager.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.manager.InternalManager;
import mx.j2.recommend.util.ClassUtil;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;

public enum InternalFilterManager implements InternalManager {
    INSTANCE;

    private HashMap<String, IStreamComponent<BaseDataCollection>> filterMap;

    InternalFilterManager() {
        filterMap = new HashMap<>();
        try {
            List<Class> classes = ClassUtil.getAllClassByInterface(Class.forName("mx.j2.recommend.filter.IFilter"));
            for (Class c : classes) {
                if (!Modifier.isAbstract(c.getModifiers())) {
                    IStreamComponent filter = (IStreamComponent) c.getConstructor().
                            newInstance();
                    filterMap.put(filter.getName(), filter);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void process(InternalDataCollection dc, OtherDataCollection otherDc) {
        for (String filterName : dc.recommendFlow.filterList) {
            if (filterMap.containsKey(filterName)) {
                filterMap.get(filterName).process(otherDc);
            }
        }
    }
}
