# -*- coding: utf-8 -*-
from extractor import nanfangplus
from extractor import infzm
from extractor import pai
import logging,time
# 配置logger
curdatetime = time.strftime('%Y%m%d', time.localtime(time.time()))
filename = 'article_extract' + curdatetime + '.log'
logger = logging.getLogger(__name__)
handler = logging.FileHandler("logging/" + filename)
formatter = logging.Formatter('[%(asctime)s]%(name)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def pai_extract():
    logger.info('starting pai_extract')
    al = pai.AriticleList()
    channel_list = [{"channelId":1,"channelName":"热点"},{"channelId":2,"channelName":"帮News"},{"channelId":6,"channelName":"城事"},{"channelId":11,"channelName":"精读"},{"channelId":4,"channelName":"娱塘"},{"channelId":8,"channelName":"同城圈"},{"channelId":9,"channelName":"买楼帮"},{"channelId":10,"channelName":"直播"}]
    for channel in channel_list:
        al.extract_pai(channel['channelId'])

def nanafang_extract():
    logger.info('starting nanafang_extract')
    nanfang = nanfangplus.NanfangIE()
    channel_id = []
    li = nanfang._get_channel_list()
    for item in li:
        channel_id.append(item['cid'])
    print channel_id
    for chanId in channel_id:
        nanfang.extract_title(chanId)

def bingdu_extract():
    logger.info('starting bingdu_extract')
    be = pai.AriticleList()
    ch_list = [{'id':103,'name':'粤秀'},{'id':93,'name':'娱乐'},{'id':96,'name':'体育'},{'id':91,'name':'财经'},{'id':101,'name':'南方'},{'id':99,'name':'地产'},
                   {'id':90,'name':'汽车'},{'id':83,'name':'美食'},{'id':95,'name':'生活'},{'id':97,'name':'健康'},{'id':84,'name':'影视'},
                  {'id':86,'name':'游戏'},{'id':78,'name':'旅游'},{'id':79,'name':'科技'},{'id':105,'name':'华语'},{'id':102,'name':'热门'}]
    for channel in ch_list:
        be.extract_bingdu(channel['id'])

def infzm_extract():
    logger.info('starting infzm_extract')
    infzz = infzm.Extractor()
    infzz.ex_start()

# infzm_extract()
pai_extract()
nanafang_extract()
bingdu_extract()

'''
src_url = 'https://3g.k.sohu.com/t/n215989124'
sohuIE = sohu.SohuIE()
patten = r'/t/n\d*'
target = re.findall(patten, src_url)
if target:
    newsId = src_url.split('t/n')[-1]
    print newsId
    rst,len = sohuIE._handle_comment(newsId)
    print rst,len
    '''



