package mx.j2.recommend.manager.impl;

import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.manager.InternalManager;
import mx.j2.recommend.packer.InternalPacker;
import mx.j2.recommend.util.ClassUtil;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;

public enum InternalPackerManager implements InternalManager {
    INSTANCE;

    private HashMap<String, InternalPacker> packerMap;

    InternalPackerManager() {
        packerMap = new HashMap<>();
        try {
            List<Class> classes = ClassUtil.getAllClassByInterface(Class.forName("mx.j2.recommend.packer.InternalPacker"));
            for (Class c : classes) {
                if (!Modifier.isAbstract(c.getModifiers())) {
                    InternalPacker packer = (InternalPacker) c.getConstructor().
                            newInstance();
                    packerMap.put(packer.getName(), packer);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void process(InternalDataCollection dc, OtherDataCollection otherDc) {
        if (packerMap.containsKey(dc.recommendFlow.packer)) {
            packerMap.get(dc.recommendFlow.packer).pack(dc, otherDc);
        }
    }
}
