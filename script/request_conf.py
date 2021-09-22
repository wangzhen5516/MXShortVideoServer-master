# -*- coding: utf-8 -*-
# @Time    : 2017/11/23 下午1:12
# @Author  : yahui yan


recommend_request_list = [
    {
        'interfaceName': 'same_episodes_version_4_0',
        'id': 'ffd4ba799b24d4b2833e276f5ad04591',
        'type': 0,
        'num': 5
    },
    {
        'interfaceName': 'seasons_version_4_0',
        'channelId': '2ead4f6ee7d16cde2af6df5f6f53d489',
        'num': 5
    },
    {
        'interfaceName': 'episodes_one_season_version_4_0',
        'channelId': '2ead4f6ee7d16cde2af6df5f6f53d489',
        'type': 0,
        'num': 5
    },
    {
        'interfaceName': 'episodes_of_the_season_version_4_0',
        'albumId': '254b5867fc74973711f8cc8e26449f23',
        'type': 0,
        'num': 5
    },
]

getDetails_request_list = [
    {
        'ids': ['2ead4f6ee7d16cde2af6df5f6f53d489'],
        'type': 'tv_show'
    },
]

fetchTabs_request_list = [
    {
        'interfaceName': 'mxbeta_version_4_0',
        'uuid': '111111'
    },
]

getGenresList_request_list = [
    {
        'category': 'song'
    }
]

# 将要测试的接口作为键，每个键对应一个元组，元组的第二个元素是对应的请求列表，一般不需要动，
# 元组第一个元素是要打印的结果的字段，如果为空，则将打印所有字段
# 对于recommend接口，返回的是一个response，response里有resultList或detailList，所有需要配置二级字段
# 也就是result.field表示打印resultList中每个result的field字段，detailList同理
# 如果想把resultList中的每个result所有字段都打印出来，则需要只需添加'result'到列表即可
# 如果想把detailList中的每个detailInfo所有字段都打印出来，则需要添加'detailInfo'到列表
interface_for_test = {
    #'getDetails': (['id'], getDetails_request_list),
    #'fetchTabs': (['tabName'], fetchTabs_request_list),
    #'recommend': (['result.id', 'detailInfo', 'responseType'], recommend_request_list),
    #'fetchBannerData': (['bannerName'], fetchTabs_request_list)
    'getGenresList': (['name'], getGenresList_request_list)
}