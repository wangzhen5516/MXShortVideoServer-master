package mx.j2.recommend.data_model.interfaces;

import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * 文档处理回调接口
 */
@FunctionalInterface
public interface IDocumentProcessor<T extends BaseDocument> {
    void process(T document);
}
