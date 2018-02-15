# -*- coding: utf-8 -*-

import json
import codecs
import time
from scrapy.exceptions import DropItem
from model.config import DBSession,DBSession_ex,DBSession_bak
from model.config import Redis,Redis7
from model.article import Article
from model.T_news import t_news
from model.T_labels import T_labels
from model.T_newsoutinfo import T_newsoutinfo
from model.T_comment import T_comment
from model.T_completeNews import T_completeNews
from model.T_completeComments import T_completeComments
#from scrapy import signals, log
from posterpic import getLocalServerUrl
from model.T_studylabels import T_studylabels
from model.commonfunc import str_hash,checkreleasetime
from model.T_newsbrowse_analyze import T_newsbrowse_analyze
from model.commonfunc import marklabelbyplatform
from model.T_completeComments_yq import T_completeComments_yq
from model.T_yqcomments_info import T_yqcomments_info

import random
import urllib2,urllib
from model.T_media import T_media
DEBUG_MODE =0
DEFAULT_RANDOM_TIME = 3600 * 2    #2H
COMMENT_GIVEUP_TIME = 3600 * 36 #24H
COMMENT_LEAST_COUNTER = 131
BEGIN_MIN_SECOND = 25 * 60
PLUS_MIN_SECOND = 20 * 60
#LABBEL_DICT = {13:"1333",17:"1360",12:"1026",2:"1362"}
LABBEL_DICT = {13:"1333",17:"1360",12:"1368",2:"1362",19:"1363",16:"1364",1:"1365",23:"1366",4:"1367",10:"1351",21:"1369",9:"1370",8:"1413",6:"1414",7:"1416",500:"5000",46:"1452"}
#MEDIA_DICT = {12:"83939;84898;"}
MEDIA_DICT = {}
DEFAULT_LABEL = [1333,1360,1368,1362,1363,1364,1365,1366,1367,1351,1369,1370,1413,1414]
LOG_DEAFULT = '/data/pylog/scrapy_svn/'

#如果channel_id在MEDIA_DICT，说明要限制，不在MEDIA_DICT的返回空；如果不再，说明不需要限制，直接返回LABBEL_DICT的值
def getLabel_lv2(channel_id,tmedia_id):
    dmedia_str = MEDIA_DICT.get(channel_id)
    if(dmedia_str) :
        media_list = dmedia_str.split(';')
        #合法的media,取LABBEL_DICT
        if str(tmedia_id) in media_list:
            label_lv2 = LABBEL_DICT.get(channel_id)
        else:
            label_lv2 = None
    else:
        label_lv2 = LABBEL_DICT.get(channel_id)
    return label_lv2

def checkTmedia(name,platformid,channelid):
    session = DBSession()

    tmedia = session.query(T_media.id).filter(T_media.platform_id  == platformid, T_media.name == name)
    if tmedia and tmedia.count() > 0:
        id = tmedia[0].id
    else:
        t = T_media(name = name,
                               platform_id = platformid,
                               kind = channelid,
                       )
        session.add(t)
        session.commit()
        id = t.id
    session.close()
    return id

def label_hit(tag_list,channel_id,unuselv2_labels):
    #print unuselv2_labels
    db_ex = DBSession_ex()
    max_count = 0
    max_l2 = 0
    for tagitem in tag_list:
        #hash_key = str_hash(tagitem)
        #filter lv1 lv2 name
        if (tagitem == '' or  tagitem == ' 'or  tagitem ==u'　'):
            continue
     
        label_count_list = db_ex.query(T_studylabels.label_id_l2,T_studylabels.count).filter(T_studylabels.status == 1, T_studylabels.name  == tagitem, T_studylabels.label_id_l1 ==  channel_id)
        #找出次数最多的二级标签,忽略默认二级表情
        tmp_max_count = 0
        tmp_max_l2 = 0
        for item_count in label_count_list:
            #过滤所有默认二级标签，认为不是正确的学习路径
            if(item_count.label_id_l2 in DEFAULT_LABEL) :
                continue
            if(item_count.label_id_l2 in unuselv2_labels) :
                continue

            if (item_count.count > max_count):
                tmp_max_count = item_count.count
                tmp_max_l2 = item_count.label_id_l2
        if (tmp_max_count > max_count):
                max_count = tmp_max_count
                max_l2 = tmp_max_l2
    db_ex.close()
    return max_l2,max_count

def checkLabels(str_labels_str,channel_id,tmedia_id,default_labels_list,default_channel_list,curdatetime,unuselv2_labels):
    label_lv2 = getLabel_lv2(channel_id,tmedia_id)
    label_lv1 = str(channel_id)
    if str_labels_str == '':
        str_labels = str(channel_id) + ';'
        if (label_lv2):
            str_labels += (label_lv2+ ';')
        return str_labels
    else:
        max_l2 = 0
        session = DBSession()
        tag_list = str_labels_str.split(';')
        #tag_list.reverse()
        study_list = {23,21,16,13,10,9,1}
        
        #添加的三级标签，如果和一级、二级标签名字相同，则不处理
        #l1_name = session.query(T_labels.label_name).filter(T_labels.label_id  == channel_id)
        #l2_name_list = session.query(T_labels.label_name).filter(T_labels.parent_id  == channel_id)
        
        #print tag_list 
        #for item_name in tag_list:
            #if item_name in default_channel_list:
                #print item_name
                #tag_list.remove(item_name)
        #tag_list.reverse() 
        i = 0 
        for i in range(len(tag_list)):
            if tag_list[i] in default_channel_list:
                #print item_name
                tag_list[i] = ''
    
        max_l2,max_count = label_hit(tag_list,channel_id,unuselv2_labels)
        #匹配的，入库前删除与二级同名标签，否则保留
        if(max_l2 != 0):
            label_lv2 = str(max_l2)
            print 'hit:%s' % label_lv2
            for i in range(len(tag_list)):
                if tag_list[i] in default_labels_list:
                    #print item_name
                    tag_list[i] = ''
        else:
            print 'miss:%s' % label_lv2

        str_labels = label_lv1 + ';' + label_lv2 +';'
        for tagitem in tag_list:
            
            if (tagitem == '' or  tagitem == ' 'or  tagitem ==u'　'):
                continue
            #print tagitem

            tlabel = session.query(T_labels.label_id).filter(T_labels.parent_id  == label_lv2, T_labels.label_name == tagitem)
            if tlabel and tlabel.count() > 0:
                str_labels += (str(tlabel[0].label_id) + ';')
            else:
                #if (tagitem == u'科技' or tagitem == u'娱乐'or tagitem == u'军事' or tagitem == u'财经' or tagitem == u'国际' or tagitem == u'国内' or tagitem == u'汽车' or tagitem == u'体育' or tagitem == u'情感' or tagitem == u'历史' or tagitem == u'时尚' or tagitem == u'生活'):
                    #continue
                #if l1_name[0] == tagitem or (tagitem in l2_name_list):
                    #continue
                t = T_labels(label_name = tagitem,
                         parent_id = label_lv2,
                         is_use = 0,
                         crete_time =  curdatetime,
                           )
                session.add(t)
                session.commit()
                str_labels += (str(t.label_id)+ ';')
        session.close()
        return str_labels

#PGC标签，无抓到标签，返回一级+二级，如：老人;老人健康;  有的话在此基础上直接添加抓回来的标签
def calcPgcLabels(str_labels_str,channel_id,defaultlv2id,curdatetime):

    #校验，defaultlv2id必须存在且是
    #label_lv2 = getLabel_lv2(channel_id,tmedia_id)
    str_labels = str(channel_id) + ';'
    if (defaultlv2id):
        str_labels += (str(defaultlv2id)+ ';')
    if str_labels_str == '':
        return str_labels
    else:
        session = DBSession()
        tag_list = str_labels_str.split(';')

        for tagitem in tag_list:
            if (tagitem == '' or  tagitem == ' 'or  tagitem ==u'　'):
                continue

            tlabel = session.query(T_labels.label_id).filter(T_labels.parent_id  == defaultlv2id, T_labels.label_name == tagitem)
            if tlabel and tlabel.count() > 0:
                str_labels += (str(tlabel[0].label_id) + ';')
            else:
                t = T_labels(label_name = tagitem,
                         parent_id = defaultlv2id,
                         is_use = 0,
                         crete_time =  curdatetime,
                           )
                session.add(t)
                session.commit()
                str_labels += (str(t.label_id)+ ';')
        session.close()
        return str_labels


#getCommentlistbyNewsid，获取的是T_completecomment里面的数据，后续进行查重
def getCommentlistbyNewsid(newsid):
    session = DBSession_ex()
    #tcomments = session.query(T_comment.comment_id, T_comment.news_id, T_comment.content, T_comment.user_id).filter(T_comment.news_id  == newsid)
    tcomments = session.query( T_completeComments.news_id, T_completeComments.content).filter(T_completeComments.news_id  == newsid)
    list= []
    counter = 0
    for item in tcomments:
        comtmp={
            'news_id':item.news_id,
            #'comment_id':item.comment_id,
            #'content':item.content,
            #'user_id':item.user_id,
            'content_hash':hash(item.content),
        }
        counter += 1
        list.append(comtmp);

    session.close()
    return list,counter

#getCommentlistbyNewsid_EX，获取的是T_comment里面的数据，后续进行查重
def getCommentlistbyNewsid_EX(newsid):
    session = DBSession()
    #tcomments = session.query(T_comment.comment_id, T_comment.news_id, T_comment.content, T_comment.user_id).filter(T_comment.news_id  == newsid)
    tcomments = session.query( T_comment.news_id, T_comment.content).filter(T_comment.news_id  == newsid)
    list= []
    counter = 0
    for item in tcomments:
        comtmp={
            'news_id':item.news_id,
            #'comment_id':item.comment_id,
            #'content':item.content,
            #'user_id':item.user_id,
            'content_hash':hash(item.content),
        }
        counter += 1
        list.append(comtmp);

    session.close()
    return list,counter
#降低生产表压力，用T_completeComments 表进行查重
def getCommentlistbyNewsidbyEX(newsid):
    session = DBSession_ex()
    #tcomments = session.query(T_comment.comment_id, T_comment.news_id, T_comment.content, T_comment.user_id).filter(T_comment.news_id  == newsid)
    tcomments = session.query( T_completeComments.news_id, T_completeComments.content).filter(T_completeComments.news_id  == newsid, T_completeComments.ischecked == 9)
    list= []
    counter = 0
    for item in tcomments:
        comtmp={
            'news_id':item.news_id,
            #'comment_id':item.comment_id,
            #'content':item.content,
            #'user_id':item.user_id,
            'content_hash':hash(item.content),
        }
        counter += 1
        list.append(comtmp);

    session.close()
    return list,counter



def getnewcomment(newcomment_list,dbcomment_list):
    list  = []
    new_counter = 0
    for itemnew in newcomment_list:
        isSamed = 0
        for itemdb in dbcomment_list:
            #相同描述
            if itemdb['content_hash'] == itemnew['content_hash']:
                isSamed = 1
                break
        #找不同相同的hash,认为是新评论
        if not (isSamed) :
            new_counter += 1
            comtmp={
                'content':itemnew['content'],
            }
            list.append(comtmp);
    return list,new_counter

def getnewcommentex(newcomment_list,dbcomment_list):
    list  = []
    new_counter = 0
    for itemnew in newcomment_list:
        isSamed = 0
        for itemdb in dbcomment_list:
            #相同描述
            if itemdb['content_hash'] == itemnew['content_hash']:
                isSamed = 1
                break
        #找不同相同的hash,认为是新评论
        if not (isSamed) :
            new_counter += 1
            list.append(itemnew);
    return list,new_counter

def insertCompleteComment(newsid,str,parent_id,release_time):
    session = DBSession_ex()
    t = T_completeComments(news_id  = newsid,
                        news_releasetime = release_time,
                        content = str,
                        parent_id = parent_id,
                        ischecked = 0,
                        status = 0
                       )
    session.add(t)
    session.commit()
    session.close()

def getnewcomment_EX(newcomment_list,dbcomment_list):
    list  = []
    BANWORDS= ['新浪','网易','头条','一点资讯']
    new_counter = 0
    for itemnew in newcomment_list:
        isSamed = 0
        for itemdb in dbcomment_list:
            #相同描述
            if itemdb['content_hash'] == itemnew['content_hash']:
                #itemnew['ischecked'] = 8 #重复的
                #重复的update一次completecommet，下次不再查
                updateCompleteCommentStatusToDb(itemnew['id'],8,None)
                isSamed = 1
                break
        for iban in BANWORDS:
            if iban in itemnew['content'].encode("utf-8"):
                isSamed = 1
                updateCompleteCommentStatusToDb(itemnew['id'],11,None)
                break

        #找不同相同的hash,认为是新评论
        if not (isSamed) :
            new_counter += 1
            comtmp={
                'content':itemnew['content'],
                'id':itemnew['id'],
                'parent_id':itemnew['parent_id'],
            }
            list.append(comtmp);
    return list,new_counter
#此方法是返回当前时间- 5~20分钟随机时间（系统时间本身快5分钟，故最少5MIN）
def getRandomTime_EX (lastSecond,systemtime):
    #systemtime = time.time()
    #timestamp = time.mktime(time.strptime(release_time,'%Y-%m-%d %H:%M:%S'))
    if (lastSecond > PLUS_MIN_SECOND):
        lastSecond = PLUS_MIN_SECOND
    """
    if (systemtime - timestamp < DEFAULT_RANDOM_TIME) and (systemtime > timestamp) :
        maxrandomsecond = systemtime - timestamp
    else:
        maxrandomsecond = DEFAULT_RANDOM_TIME
    """
    #系统前25MIN的时间
    basetime = systemtime - BEGIN_MIN_SECOND

    #生成比上次更晚的时间
    b_list = range(lastSecond,PLUS_MIN_SECOND)

    ranNum = random.sample(b_list, 1)
    new_releasetime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(basetime + ranNum[0]))
    return new_releasetime,ranNum[0]

#此方法是返回 系统时间-25分钟 +（0，20MIN）随机秒的时间，且保证下一条的插入时间肯定晚于上一条
def getRandomTime (release_time):
    systemtime = time.time()
    timestamp = time.mktime(time.strptime(release_time,'%Y-%m-%d %H:%M:%S'))

    if (systemtime - timestamp < DEFAULT_RANDOM_TIME) and (systemtime > timestamp) :
        maxrandomsecond = systemtime - timestamp
    else:
        maxrandomsecond = DEFAULT_RANDOM_TIME

    b_list = range(0,maxrandomsecond)

    ranNum = random.sample(b_list, 1)
    new_releasetime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(timestamp + ranNum[0]))
    return new_releasetime

def giveupcomment(release_time):
    systemtime = time.time()
    timestamp = time.mktime(time.strptime(release_time,'%Y-%m-%d %H:%M:%S'))
    if systemtime - timestamp > COMMENT_GIVEUP_TIME:
        return True
    else:
        return False

def getRandomUser():
    b_list = range(417,65971)

    ranNum = random.sample(b_list, 1)
    return ranNum[0]

def updateArticleStatusToDb(id,url_status):
    session = DBSession()
    session.query(Article.id).filter(Article.id  == id).update({Article.isdeleted: url_status})
    session.commit()
    session.close()

def updateNewsStatusToDb(id,url_status):
    session = DBSession()
    session.query(t_news.news_id,t_news.isdeleted).filter(t_news.news_id  == id).update({t_news.isdeleted: url_status})
    session.commit()
    session.close()


def updateCompleteCommentStatusToDb(id,ischecked,comment_time):
    session = DBSession_ex()
    if comment_time:
        session.query(T_completeComments.id).filter(T_completeComments.id  == id).update({T_completeComments.ischecked: ischecked,T_completeComments.commit_time: comment_time})
    else:
        session.query(T_completeComments.id).filter(T_completeComments.id  == id).update({T_completeComments.ischecked: ischecked})
    session.commit()
    session.close()

def handlecomment(item,news_id,mode,channel_id):
        #id =440
    e = ()
    try:
        comments = 0
        #release_time = item['release_time']
        release_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        dbcomment_list,db_counter = getCommentlistbyNewsid(news_id)
        #dbcomment_list =[]
        #db_counter =0
        #新数据与T_completecommet 的查重
        newcomment_list,new_counter = getnewcomment(item['comment_list'],dbcomment_list)
        #COMMENT_LEAST_COUNTER
        #comment_counter = db_counter + new_counter
        #print dbcomment_list
        #print newcomment_list
        for comm in newcomment_list:
            str =  comm['content'].encode('utf8')
            #log.msg(str, level=log.INFO)
            #print 'newsid:%s' % news_id +  ':%s' % str
            insertCompleteComment(news_id,str,0,release_time)
            comments += 1
            time.sleep(0.1)
            """
            userid = getRandomUser()
            comment_time = getRandomTime(release_time)

            url =  'xwapi.iimedia.cn/5/index.action?action=comment&params={"content":"%s'% str + '","news_id":"%s"'% news_id  + ',"parent_id":"0","time":"%s"'% comment_time + ',"user_id":"%d' % userid +'"}'
            url = urllib.quote_plus(url,"\"?=&!/")
            url = 'http://' + url
            if DEBUG_MODE:
                print url


                request= urllib2.Request(url)
                str = urllib2.urlopen(request).read()
                if str.find("SUCCESS") :
                    comments = comments+ 1
                    time.sleep(1)
                    url_browse =  'xwapi.iimedia.cn/3/index.action?action=recordBehavior&params={"channel_id":%d'% channel_id + ',"labels":"","news_id":%s'% news_id +',"op":"browse_news","user_id":%s}' % userid
                    url_browse = urllib.quote_plus(url_browse,",:?=&!{}/")
                    url_browse = 'http://' + url_browse
                    print url_browse
                    request= urllib2.Request(url_browse)
                    str = urllib2.urlopen(request).read()

                time.sleep(1)
        """


        #newcomment_list为空，说明没有评论，或没有新评论，在24小时内，不记标记，下次可继续抓；如果超过24小时，表示该文章/视频冷门，不再进行评论抓取
        if (newcomment_list == []) and giveupcomment(release_time):
            if mode == 3 and not DEBUG_MODE:
                updateArticleStatusToDb(news_id,4)
            elif mode == 2  and not DEBUG_MODE:
                updateNewsStatusToDb(news_id,4)
        #抓到了评论+db已有评论>COMMENT_LEAST_COUNTER,认为抓够了，置DB标记为2
        elif comments + db_counter > COMMENT_LEAST_COUNTER:
            if mode == 3 and not DEBUG_MODE:
                updateArticleStatusToDb(news_id,2)
            elif mode == 2  and not DEBUG_MODE:
                updateNewsStatusToDb(news_id,2)
    except Exception as e:
        pass
        #except状态，不再更新数据库：如未插入过数据，下次执行脚本重新取；插入过下次也会判断插入条数并置对应标志
        #if comments != 0 and not DEBUG_MODE:
            #updateArticleStatusToDb(news_id,4) #若1条都未插入就报HTTP错，先置0
        print e

def handleCompleteComments(commist_list,mode,news_id):
    e = ()
    try:
        comments = 0
        #获取已呈现的评论
        dbcomment_list,db_counter = getCommentlistbyNewsidbyEX(news_id)
        #print dbcomment_list
        #T_completecommet与T_commet 之间的查重
        newcomment_list,new_counter = getnewcomment_EX(commist_list,dbcomment_list)
        #print  newcomment_list
        lastSecond = 0
        systemtime = time.time()
        for comm in newcomment_list:
            str =  comm['content'].encode('utf8')
            parentid = comm['parent_id']
            userid = getRandomUser()
            if mode > 0:
                #返回一个略早于系统时间的时间
                comment_time,lastSecond = getRandomTime_EX(lastSecond,systemtime)
            else:
                comment_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(systemtime))

            #url =  'xwapi.iimedia.cn/5/index.action?action=comment&params={"content":"%s'% str + '","news_id":"%s"'% news_id  + ',"parent_id":"%s"' %parentid + ',"time":"%s"'% comment_time + ',"user_id":"%d' % userid +'"}'
            url =  '10.0.0.6:9414/5/index.action?action=comment&params={"content":"%s'% str + '","news_id":"%s"'% news_id  + ',"parent_id":"%s"' %parentid + ',"time":"%s"'% comment_time + ',"user_id":"%d' % userid +'"}'
            #print url
            #log.msg(url, level=log.INFO)
            url = urllib.quote_plus(url,"\"?=&!/")
            url = 'http://' + url
            #timea = time.time()
            request= urllib2.Request(url)
            str = urllib2.urlopen(request).read()
            #timeb = time.time()
            #print 'cost:%s' % (timeb - timea)
            if str.find("SUCCESS") :
                updateCompleteCommentStatusToDb(comm['id'],9,comment_time)#更新comment表
                comments = comments+ 1
            print news_id
            """
            if DEBUG_MODE:
                print url
            else:
                request= urllib2.Request(url)
                str = urllib2.urlopen(request).read()
                if str.find("SUCCESS") :
                    comments = comments+ 1
            """
            #time.sleep(0.3)
        """
        #newcomment_list为空，说明没有评论，或没有新评论，在24小时内，不记标记，下次可继续抓；如果超过24小时，表示该文章/视频冷门，不再进行评论抓取
        if (newcomment_list == '') and giveupcomment(release_time):
            if mode == 3 and not DEBUG_MODE:
                updateArticleStatusToDb(news_id,4)
        #抓到了评论+db已有评论>COMMENT_LEAST_COUNTER,认为抓够了，置DB标记为2
        elif comments + db_counter > COMMENT_LEAST_COUNTER:
            if mode == 3 and not DEBUG_MODE:
                updateArticleStatusToDb(news_id,2)
        """
        return comments

    except Exception as e:
        pass
        #except状态，不再更新数据库：如未插入过数据，下次执行脚本重新取；插入过下次也会判断插入条数并置对应标志
        #if comments != 0 and not DEBUG_MODE:
            #updateArticleStatusToDb(news_id,4) #若1条都未插入就报HTTP错，先置0
        print e
        return comments

def mapsizelarger(size):
    e = ()
    if not size:
        return False
    try:
        if size[0] > 375 and size[1] > 225:
            return True
        else:
            return False
    except:
        pass
        return False

def uploadNewsImage(item):
    print '***********************************************uploadNewsImage'
    content = item['content']
    channel_id = item['channel_id']
    imglist2 = content.find_all("img")
    str = ''
    titlepiclist = []
    images_path = ''
    for img in imglist2:
        if img.has_key("onerror"):
            img.__delitem__("onerror")
        if img.has_key("img_width"):
            img.__delitem__("img_width")
        if img.has_key("img_height"):
            img.__delitem__("img_height")
        if img.has_key("data-src"):
            img.__delitem__("data-src")
        if img.has_key("data-info"):
            img.__delitem__("data-info")
        str=  img.get('src')
        if str:            
            img['src'],size,format= getLocalServerUrl(str,channel_id)
               
            if  mapsizelarger(size) :
                tmppic = img['src'] + '?t=1'
                titlepiclist.append(tmppic)
            if format == 1:
                img['src'] = img['src'] + '?p=0' 
    if len(titlepiclist) == 0:
        titlepic = ''
    elif len(titlepiclist) < 3:
        titlepic = titlepiclist[0]
    else:
        titlepic = titlepiclist[1]
    lenpath = 0
    if len(titlepiclist) > 3:
        for pic in titlepiclist:
            if lenpath == 3:
                break
            if titlepic != pic:
                lenpath += 1
                images_path += (pic + '|#|#|')

    item['images_path'] = images_path
    item['content'] = content
    item['image']   = titlepic
    print titlepic,images_path
    return item
#去头 
def uploadNewsImage_spec(item):
    print '***********************************************uploadNewsImage'
    content = item['content']
    channel_id = item['channel_id']
    imglist2 = content.find_all("img")
    str = ''
    titlepiclist = []
    images_path = ''
    count = 0
    for img in imglist2:
        count += 1
        if img.has_key("onerror"):
            img.__delitem__("onerror")
        if img.has_key("img_width"):
            img.__delitem__("img_width")
        if img.has_key("img_height"):
            img.__delitem__("img_height")
        if img.has_key("data-src"):
            img.__delitem__("data-src")
        if img.has_key("data-info"):
            img.__delitem__("data-info")
        str=  img.get('src')
        if str:
            if count == 1:
                img.decompose()
                print 'give up %s' % str
                continue
            img['src'],size,format= getLocalServerUrl(str,channel_id)

            if  mapsizelarger(size) :
                tmppic = img['src'] + '?t=1'
                titlepiclist.append(tmppic)
            if format == 1:
                img['src'] = img['src'] + '?p=0'
    if len(titlepiclist) == 0:
        titlepic = ''
    elif len(titlepiclist) < 3:
        titlepic = titlepiclist[0]
    else:
        titlepic = titlepiclist[1]
    lenpath = 0
    if len(titlepiclist) > 3:
        for pic in titlepiclist:
            if lenpath == 3:
                break
            if titlepic != pic:
                lenpath += 1
                images_path += (pic + '|#|#|')

    item['images_path'] = images_path
    item['content'] = content
    item['image']   = titlepic
    print titlepic,images_path
    return item

#掐头去尾
def uploadNewsImage_spec22(item):
    print '***********************************************uploadNewsImage'
    content = item['content']
    channel_id = item['channel_id']
    imglist2 = content.find_all("img")
    str = ''
    titlepiclist = []
    images_path = ''
    count = 0
    lenlist = len(imglist2)
    for img in imglist2:
        count += 1
        if img.has_key("onerror"):
            img.__delitem__("onerror")
        if img.has_key("img_width"):
            img.__delitem__("img_width")
        if img.has_key("img_height"):
            img.__delitem__("img_height")
        if img.has_key("data-src"):
            img.__delitem__("data-src")
        if img.has_key("data-info"):
            img.__delitem__("data-info")
        str=  img.get('src')
        if str:
            if count == 1:
                img.decompose()
                print 'give up head %s' % str
                continue
            if count == lenlist:
                img.decompose()
                print 'give up tail %s' % str
                continue
            img['src'],size,format= getLocalServerUrl(str,channel_id)

            if  mapsizelarger(size) :
                tmppic = img['src'] + '?t=1'
                titlepiclist.append(tmppic)
            if format == 1:
                img['src'] = img['src'] + '?p=0'
    if len(titlepiclist) == 0:
        titlepic = ''
    elif len(titlepiclist) < 3:
        titlepic = titlepiclist[0]
    else:
        titlepic = titlepiclist[1]
    lenpath = 0
    if len(titlepiclist) > 3:
        for pic in titlepiclist:
            if lenpath == 3:
                break
            if titlepic != pic:
                lenpath += 1
                images_path += (pic + '|#|#|')

    item['images_path'] = images_path
    item['content'] = content
    item['image']   = titlepic
    print titlepic,images_path
    return item



class DuplicatesPipeline(object):
    #close_spider(spider, reason='finished')
    def __init__(self):
        self.count = 50
    def process_item(self, item, spider):

        title = item['title']
        if spider.rule.enable is not 2:
            publish_time = item['publish_time']
            source_site  = item['source_site']
            #print title + source_site + publish_time
            if ( title == '' ) or (( publish_time =='')  and  ( source_site == '')):
                #log.msg("Drop none info data! url: %s" % item['url'], level=log.INFO)
                raise DropItem("Drop none info data! url: %s" % item['url'])
            videourlhash = item['url_id']


            if self.count == 0:
                #log.msg("Duplicate too much, stop and wait for update: %s" % spider, level=log.INFO)
                spider.crawler.engine.close_spider(spider, reason='finished')
                raise DropItem("Duplicate too much, last item: %s" % item['url'])
                #close_spider(spider, reason='finished')
            if not DEBUG_MODE:
                if Redis.exists('iimedia_127_channelid_urlHashSet_%d' % videourlhash):
                    #Redis.delete('iimedia_127_channelid_urlHashSet:%d' % videourlhash )
                    #log.msg("Same record! hashs: %d" % videourlhash, level=log.INFO)
                    self.count -= 1
                    raise DropItem("Duplicate item found: %s" % item['url'])
                else:
                    Redis.set('iimedia_127_channelid_urlHashSet_%d' % videourlhash, 1)
        else:
            content = item['content']
            image  = item['image']
            if (not title ) and ((not content)  or  (not image)):
                raise DropItem("Drop none info data! url: %s" % item['url'])
            if self.count == 0:
                #log.msg("Duplicate too much, stop and wait for update: %s" % spider, level=log.INFO)
                spider.crawler.engine.close_spider(spider, reason='finished')
                raise DropItem("Duplicate too much, last item: %s" % item['url'])
        return item

class DuplicatesPipelinefordrm(object):
    #close_spider(spider, reason='finished')
    def __init__(self):
        self.count = 8
    """    
    def open_spider(self, spider):
        self.urls_seen = set()
        list = spider.list;
        for item in list:
            self.urls_seen.add(hash(item['src_url']))
        print '1'
    """
    def process_item(self, item, spider):

        title = item['title']
        content = item['content']
        image  = item['image']
        if ( title == '' ) or ( content ==''):
            raise DropItem("Drop none info data! url: %s" % item['src_url'])
        if self.count == 0:
            #log.msg("Duplicate too much, stop and wait for update: %s" % spider, level=log.INFO)
            spider.crawler.engine.close_spider(spider, reason='finished')
            raise DropItem("Duplicate too much, last item: %s" % item['src_url'])
        """
        for urlitem  in self.urls_seen:
            if (urlitem == hash(item['src_url'])):
                self.count -= 1
                raise DropItem("urls_seen, url: %s" % item['src_url'])
        else:
          self.urls_seen.add(hash(item['src_url']))
        """
        return item

class DuplicatesPipeline_ex(object):
    #close_spider(spider, reason='finished')
    def __init__(self):
        self.count = 3
        self.outdatecount = 10
    def process_item(self, item, spider):

        title = item['title']
        if spider.rule.enable == 6:
            if ( title == '' ):
                raise DropItem("Drop none info data! url: %s" % item['src_url'])
            uhash = hash(item['src_url'])
            release_time = item['release_time']
            release_timestamp = time.mktime(time.strptime(release_time,'%Y-%m-%d %H:%M:%S'))
            currentstamp = time.time()
            if self.count == 0:
                spider.crawler.engine.close_spider(spider, reason='finished')
                raise DropItem("Duplicate too much, last item: %s" % item['src_url'])
            #if self.outdatecount == 0:
                #spider.crawler.engine.close_spider(spider, reason='finished')
                #raise DropItem("Outdatecount too much, last item: %s" % item['src_url'])

            for item2 in spider.list:
                if uhash == item2['src_url_hash']:
                    print '************* guest is duplicate!!!'
                    self.count -= 1
                    raise DropItem("Duplicate item found: %s" % item['src_url'])
            #超过三个月，不再抓取
            if currentstamp - release_timestamp > 3 * 30 * 24 * 3600:
                raise DropItem("Outdatecount too much, last item: %s" % item['src_url'])
                #self.outdatecount -= 1

        return item

# 存储到数据库
class DataBasePipeline(object):
    def open_spider(self, spider):
        self.session = DBSession()


    def process_item(self, item, spider):
        #print item["title"],item["src_url"],item["browse"],item["comment"]
         
        if spider.rule.enable  == 1:
            a = Article(title=item["title"].encode("utf-8"),
                        url=item["url"],
                        body=item["body"].encode("utf-8"),
                        publish_time=item["publish_time"].encode("utf-8"),
                        source_site=item["source_site"].encode("utf-8"),
                        url_id=item["url_id"],
                        label_id = item["label_id"],
                        isdeleted = 0,status = 0, enable = 0)

            if not DEBUG_MODE:
                self.session.add(a)
                self.session.commit()
            else:
                print item["url"]

        else:
            curdatetime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            if spider.rule.enable == 121:
                istimelegal = checkreleasetime(curdatetime,item["release_time"])
                if not istimelegal:
                    return
            if spider.rule.id == 993:
                item = uploadNewsImage_spec(item)
            elif spider.rule.id == 1008:
                item = uploadNewsImage_spec22(item)
            else:
                item = uploadNewsImage(item)
            tmedia_id = checkTmedia(item["media_name"],item["platform_id"],item["channel_id"])
            if item["platform_id"] != 200:
                if spider.rule.label_l2 is not None :
                    strlabels = str(spider.rule.label_id) + ';' + str(spider.rule.label_l2) + ';'
                    if spider.rule.id == 993:
                        strlabels += '337704;' 
                else:
                    strlabels = checkLabels(item["labels"],item["channel_id"],tmedia_id,spider.default_labels_list,spider.default_channel_list,curdatetime,spider.unuse_lv2list)
            #PGC自审核频道，采取编辑的归类作为二级，且其他抓回来的标签尝试作为三级标签
            else:
                strlabels = calcPgcLabels(item["labels"],item["channel_id"],spider.rule.label_l2,curdatetime)            
            t = t_news(platform_id=item["platform_id"],
                        media_id=tmedia_id,
                        media_name=item["media_name"],
                        channel_id=item["channel_id"],
                        labels=strlabels,
                        LEVEL=item["LEVEL"],
                        title = item["title"],
                        release_time = item["release_time"],
                        content = item["content"],
                        summary = item["summary"],
                        image = item["image"],
                        src_url = item["src_url"],
                        ischecked = item["ischecked"],
                        isdeleted = item["isdeleted"],
                        images_path = item["images_path"],
                        create_time = curdatetime,
                        #author_Id = 171,
                       )
            self.session.add(t)
            self.session.commit()
            marklabelbyplatform(strlabels,item["src_url"],self.session)
            newsid = t.news_id
            comp = T_completeNews(news_id= newsid,channel_id=item["channel_id"],create_time = curdatetime)
            self.session.add(comp)
            self.session.commit()
            #self.handlecomment(item,newsid,item["release_time"])
            comment_num = -1
            readed_num = 0
            if hasattr(spider,'newscommentlist') and spider.newscommentlist:
                print item["src_url"]
                for nitem in spider.newscommentlist:
                    if nitem['source_url'] == item["src_url"]:
                        comment_num = nitem['comments_count']
                        if nitem.has_key('readed_count'):
                            readed_num = nitem['readed_count']

                if comment_num > -1:
                    tout = T_newsoutinfo(news_id= newsid,channel_id=item["channel_id"],create_time = curdatetime,platform_id=item["platform_id"],comment_count=comment_num,browse_count=readed_num,src_url=item["src_url"])
                    self.session.add(tout)
                    self.session.commit()
    
    def close_spider(self,spider):
        self.session.close()

class DataBasePipeline_ex(object):
    def open_spider(self, spider):
        self.session = DBSession()
        self.session_ex = DBSession_ex()


    def process_item(self, item, spider):
        print item["src_url"],item["browse"],item["comment"]
        
        if spider.rule.enable  == 1:
            a = Article(title=item["title"].encode("utf-8"),
                        url=item["url"],
                        body=item["body"].encode("utf-8"),
                        publish_time=item["publish_time"].encode("utf-8"),
                        source_site=item["source_site"].encode("utf-8"),
                        url_id=item["url_id"],
                        label_id = item["label_id"],
                        isdeleted = 0,status = 0, enable = 0)

            if not DEBUG_MODE:
                self.session.add(a)
                self.session.commit()
            else:
                print item["url"]

        else:
            curdatetime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            if spider.rule.enable == 121:
                istimelegal = checkreleasetime(curdatetime,item["release_time"])
                if not istimelegal:
                    return
            item = uploadNewsImage(item)
            tmedia_id = checkTmedia(item["media_name"],item["platform_id"],item["channel_id"])
            if spider.rule.label_l2 is not None :
                strlabels = str(spider.rule.label_id) + ';' + str(spider.rule.label_l2) + ';'
            else:
                strlabels = checkLabels(item["labels"],item["channel_id"],tmedia_id,spider.default_labels_list,spider.default_channel_list,curdatetime,spider.unuse_lv2list)
            t = t_news(platform_id=item["platform_id"],
                        media_id=tmedia_id,
                        media_name=item["media_name"],
                        channel_id=item["channel_id"],
                        labels=strlabels,
                        LEVEL=item["LEVEL"],
                        title = item["title"],
                        release_time = item["release_time"],
                        content = item["content"],
                        summary = item["summary"],
                        image = item["image"],
                        src_url = item["src_url"],
                        ischecked = item["ischecked"],
                        images_path = item["images_path"],
                        isdeleted = item["isdeleted"],
                        create_time = curdatetime,
                        #author_Id = 171,
                       )
            self.session.add(t)
            self.session.commit()
            marklabelbyplatform(strlabels,item["src_url"],self.session)
            newsid = t.news_id
            comp = T_completeNews(news_id= newsid,channel_id=item["channel_id"],create_time = curdatetime)
            self.session.add(comp)
            self.session.commit()
            browse = T_newsbrowse_analyze(news_id= newsid,src_url=item["src_url"],browse_count = item["browse"],commnet_count = item["comment"],update_time = curdatetime)
            self.session_ex.merge(browse)
            self.session_ex.commit()
    
    def close_spider(self,spider):
        self.session.close()
        self.session_ex.close()

# 存储到文件
class JsonWriterPipeline(object):

    def __init__(self):
        self.file = codecs.open('items.json', 'w', encoding='utf-8')

    def process_item(self, item, spider):
        #A = item.fields
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line.decode('unicode_escape'))
        return item

# 爬取指定条数 100条
class CountDropPipline(object):
    def __init__(self):
        self.count = 2

    def process_item(self, item, spider):
        if self.count == 0:
            #log.msg("Over item found: %s" % spider, level=log.INFO)
            raise DropItem("Over item found: %s" % item)
        else:
            self.count -= 1
            return item


def initRedisUrlHashSetProcess():
    list = []
    session = DBSession()
    query = session.query(Article.id,Article.url_id,Article.label_id)
    indexarticle = 0
    for article in query:
        arttmp={
            'id':article.id,
            'url_id':article.url_id,
            'label_id':article.label_id,
        }
        indexarticle += 1
        list.append(arttmp);
    #print "handled %d record into redis" % counter
    #log.msg("indexarticle: %s" % indexarticle, level=log.INFO)
    print "indexarticle %d" % indexarticle
    counter = 0

    for item in list:
        if not DEBUG_MODE:
            if Redis.exists('iimedia_127_channelid_urlHashSet_%d' % item['url_id']):
                #print "%s exist in redis "  % item['url_id']
                continue
            else:
                counter += 1
                Redis.set('iimedia_127_channelid_urlHashSet_%d' % item['url_id'],1)
                print 'add: iimedia_127_channelid_urlHashSet_%d' % item['url_id']
        #else:
            #print "id: %d" % item['id'] + "urlhash: %d" % item['url_id']
    print "handle redis:article %d" % counter
    session.close()
"""
def selectItemForDebug(platformid = 9):
    list = [];
    session = DBSession_bak()
    query = session.query(t_news.src_url,t_news.news_id).filter(t_news.platform_id == platformid, t_news.ischecked != 8).order_by('news_id desc').limit(3000)

    for news in query:
        arttmp={
            'src_url_hash':hash(news.src_url),
            #'news_id':news.news_id,
        }
        #listtmp.id = article.id
        #listtmp.url = article.url
        list.append(arttmp);
    query2 = session.query(t_news.src_url,t_news.news_id).filter(t_news.platform_id == platformid, t_news.ischecked == 8).order_by('news_id desc').limit(4000)

    for news2 in query2:
        arttmp={
            'src_url_hash':hash(news2.src_url),
            #'news_id':news.news_id,
        }
        #listtmp.id = article.id
        #listtmp.url = article.url
        list.append(arttmp);


    session.close()
    return list
"""
def selectItemForDebug(platformid = 9):
    list = [];
    systemtime = time.time()
    curtimestamp =  int(systemtime*1000)
    REDIS_PLATFROMID_SRCURL = 'platformid_srcurl_'
    key = REDIS_PLATFROMID_SRCURL + str(platformid)
    if platformid == 200 or platformid == 1 :
        print 'get src_url from redis'
        src_list = Redis7.zrangebyscore(key,0,curtimestamp)
        for src in src_list:
            arttmp={
                'src_url_hash':hash(src),
            }
            list.append(arttmp);
    elif platformid ==9 or platformid ==16 or platformid ==27:
       print 'get src_url from redis 9,16,27'
       listp = [9,16,27]
       for item in listp:
           tmpkey = REDIS_PLATFROMID_SRCURL + str(item)
           src_list = Redis7.zrangebyscore(tmpkey,0,curtimestamp)
           for src in src_list:
               arttmp={
                   'src_url_hash':hash(src),
               }
               list.append(arttmp);
    else:
        session = DBSession_bak()
        query = session.query(t_news.src_url,t_news.news_id).filter(t_news.platform_id == platformid, t_news.ischecked != 8).order_by('news_id desc').limit(3000)

        for news in query:
            arttmp={
                'src_url_hash':hash(news.src_url),
                #'news_id':news.news_id,
            }
            #listtmp.id = article.id
            #listtmp.url = article.url
            list.append(arttmp);
        query2 = session.query(t_news.src_url,t_news.news_id).filter(t_news.platform_id == platformid, t_news.ischecked == 8).order_by('news_id desc').limit(4000)

        for news2 in query2:
            arttmp={
                'src_url_hash':hash(news2.src_url),
                #'news_id':news.news_id,
            }
            #listtmp.id = article.id
            #listtmp.url = article.url
            list.append(arttmp);
        session.close()
    return list


def selectItemForPGC(platformid,channel_id):
    list = [];
    session = DBSession_bak()
    query = session.query(t_news.src_url,t_news.news_id).filter(t_news.platform_id == platformid,t_news.channel_id == channel_id, t_news.ischecked != 8).order_by('news_id desc').limit(3000)

    for news in query:
        arttmp={
            'src_url_hash':hash(news.src_url),
            #'news_id':news.news_id,
        }
        #listtmp.id = article.id
        #listtmp.url = article.url
        list.append(arttmp);
    query2 = session.query(t_news.src_url,t_news.news_id).filter(t_news.platform_id == platformid,t_news.channel_id == channel_id, t_news.ischecked == 8).order_by('news_id desc').limit(4000)

    for news2 in query2:
        arttmp={
            'src_url_hash':hash(news2.src_url),
            #'news_id':news.news_id,
        }
        #listtmp.id = article.id
        #listtmp.url = article.url
        list.append(arttmp);


    session.close()
    return list


def selectItemForDebug_ex(platformid = 16):
    list = [];
    session = DBSession_bak()
    query = session.query(t_news.src_url,t_news.news_id).filter(t_news.platform_id == platformid).order_by('news_id desc').limit(4000)

    for news in query:
        arttmp={
            'src_url_hash':hash(news.src_url),
            #'news_id':news.news_id,
        }
        #listtmp.id = article.id
        #listtmp.url = article.url
        list.append(arttmp);

    session.close()
    return list

class ProxyMiddleware(object):

    def process_request(self, request, spider):
        
        print '************************'
        print request
        str= "http://%s" %  spider.proxy
        print str
        request.meta['proxy'] = str

        #request.meta['proxy'] = "http://10.0.0.3:8007"

        #proxy_user_pass = "user:psw"

        #encoded_user_pass = base64.encodestring(proxy_user_pass)

        #request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass


def handlecommentdirect(item):
    comments = 0
    #release_time = item['release_time']
    release_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    dbcomment_list =[]
    db_counter =0
    #新数据与T_completecommet 的查重
    newcomment_list,new_counter = getnewcomment(item['comment_list'],dbcomment_list)

    return newcomment_list,new_counter


def getYQCommentlistbyNewsid(newsid):
    session = DBSession_ex()
    tcomments = session.query( T_completeComments_yq.news_id, T_completeComments_yq.content).filter(T_completeComments_yq.news_id  == newsid)
    list= []
    counter = 0
    for item in tcomments:
        comtmp={
            'news_id':item.news_id,
            'content_hash':hash(item.content),
        }
        counter += 1
        list.append(comtmp);
    session.close()
    return list,counter

def insertCompleteCommentyq(newsid,str,parent_id,hashcode,labels):
    session = DBSession_ex()
    release_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    t = T_completeComments_yq(news_id  = newsid,
                        commit_time = release_time,
                        content = str,
                        parent_id = parent_id,
                        ischecked = 0,
                        status = 0,
                        hash_code = hashcode,
                        labels = labels,
                        reserved = 9
                       )
    session.add(t)
    session.commit()
    id = t.id
    session.close()
    return id

def deletesamecomment(comment_list):
    commenthash_list = []
    newcomment_list = []
    for comm in comment_list:
        tmphash = hash(comm['content'])
        if tmphash not in commenthash_list:
            commenthash_list.append(tmphash)
            newcomment_list.append(comm)

    newlen = len(newcomment_list)
    return newcomment_list,newlen

#处理舆情项目的评论，存储在其他表格
def handleyqcomment(item,news_id,hashcode,labels):
    #先对item去重
    curcomment_list = item['comment_list']
    newcomment_list,newlen = deletesamecomment(curcomment_list)
    comments = 0
    release_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    dbcomment_list,db_counter = getYQCommentlistbyNewsid(news_id)
    #新数据与T_completecommet 的查重
    newcomment_list,new_counter = getnewcomment(newcomment_list,dbcomment_list)

    for comm in newcomment_list:
        str =  comm['content'].encode('utf8')
        insertCompleteCommentyq(news_id,str,0,hashcode,labels,release_time)
        comments += 1
        time.sleep(0.1)

def insertHandleComment(newsid,str,parent_id,release_time):
    session = DBSession_ex()
    t = T_completeComments(news_id  = newsid,
                        news_releasetime = release_time,
                        content = str,
                        parent_id = parent_id,
                        ischecked = 11,
                        status = 0
                       )
    session.add(t)
    session.commit()
    comid = t.id
    session.close()
    return comid


def delHandleComment(newsid,lastid):
    idlist = []
    session = DBSession_ex()
    tcomments = session.query( T_completeComments.id).filter(T_completeComments.news_id  == newsid, T_completeComments.id < lastid)
    for item in tcomments:
        idlist.append(item.id)
    session.close()
    return idlist

def insertCommentinfoyq(comment_id,nickname,comment_time,user_id,platform_id,reply_num,like_num):
    print comment_id,nickname,comment_time,user_id,platform_id,reply_num,like_num
    session = DBSession_ex()
    t = T_yqcomments_info(comment_id  = comment_id,
                        nickname = nickname,
                        comment_time = comment_time,
                        user_id = user_id,
                        platform_id = platform_id,
                        reply_num = reply_num,
                        like_num = like_num,
                       )
    session.merge(t)
    session.commit()
    session.close()


def handleyqcommentex(item,news_id,hashcode,labels):
    #先对item去重
    curcomment_list = item['comment_list']
    newcomment_list,newlen = deletesamecomment(curcomment_list)
    comments = 0
    release_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    dbcomment_list,db_counter = getYQCommentlistbyNewsid(news_id)
    #新数据与T_completecommet 的查重
    newcomment_list,new_counter = getnewcommentex(newcomment_list,dbcomment_list)

    for comm in newcomment_list:
        print comm
        str = comm['content'].encode('utf8')
        cid = insertCompleteCommentyq(news_id,str,0,hashcode,labels)
        comment_id = cid
        if comm.has_key('nickname'):
            nickname = comm['nickname']
            comment_time = comm['comment_time']
            user_id = comm['user_id']
            reply_num = comm['reply_num']
            like_num = comm['like_num']
            platform_id = comm['platform_id']
            insertCommentinfoyq(comment_id,nickname,comment_time,user_id,platform_id,reply_num,like_num)
        comments += 1
        time.sleep(0.1)
