import jieba
import pymysql
import datetime
import re
import threading

class DB(object):
    """create mysql conn"""
    def __init__(self, host=None, username=None, pwd=None, dbname=None, charset="utf8"):
        self.pool = {}
        self.host = host
        self.username = username
        self.pwd = pwd
        self.dbname = dbname
        self.charset = charset

    def get_instance(self):
        name = threading.current_thread().name
        if name not in self.pool:
            conn = pymysql.connect(self.host, self.username, self.pwd, self.dbname)
            self.pool[name] = conn
        return self.pool[name]


class cut_word(object):
    def __init__(self):
        # 创建MySQL实例
        self.local_db = DB(
            host="", username="", pwd="", dbname=""
        )
        # 数据表的一些字段
        self.user_name = "..."
        self.user_id = ".."
        self.project_name = ".."
        # 获取游戏名数组
        self.game_array = self.get_game_array()
        # 存放游戏名对应的top100分词 gamename:{"cut_word":cut_word, "count": count}
        self.user_game_word_array = {}
        self.start_index = 0
        self.end_index = 0
        # self.fen_ci = []
        # self.top_50 = []
        self.threads = []
        self.main()


    def get_user_list(self):
        # 获取用户列表
        server_db = self.local_db.get_instance()
        server_cursor = server_db.cursor(cursor=pymysql.cursors.DictCursor)
        search_user_info_sql = "select id, username from userInfo_userinfo;"
        server_cursor.execute(search_user_info_sql)
        user_list = server_cursor.fetchall()
        server_cursor.close()
        # server_db.close()
        return user_list

    def get_game_array(self):
        # 获取游戏名数组
        server_db = self.local_db.get_instance()
        server_cursor = server_db.cursor(cursor=pymysql.cursors.DictCursor)
        search_game_array_sql = "select game_name from spider_array where project_name = 'baidu';"
        server_cursor.execute(search_game_array_sql)
        game_array = server_cursor.fetchall()
        server_cursor.close()
        # server_db.close()
        return game_array

    def get_stopwords(self, username):
        # 获取停用词
        server_db = self.local_db.get_instance()
        server_cursor = server_db.cursor(cursor=pymysql.cursors.DictCursor)
        stopword_sql = 'select stopword from stopwords where username = "{0}";'.format(username)
        server_cursor.execute(stopword_sql)
        stopwords = [data["stopword"] for data in server_cursor.fetchall()]
        stopwords.append('\u3000')
        server_cursor.close()
        # server_db.close()
        return stopwords

    def get_userdict(self, username):
        # 获取用户的userdict
        server_db = self.local_db.get_instance()
        server_cursor = server_db.cursor()
        userdict_sql = 'select word, word_frequency from userdict where username = "{0}";'.format(
                        username)
        server_cursor.execute(userdict_sql)
        userdict = dict(server_cursor.fetchall())
        server_cursor.close()
        # server_db.close()
        return userdict

    def start_cut(self, username, game_name, user_stopwords, user_userdict):
        server_db = self.local_db.get_instance()
        server_cursor = server_db.cursor(cursor=pymysql.cursors.DictCursor)

        try:
            jieba.load_userdict(user_userdict)
        except IOError:
            print('userdict is not accessible.')

        array = {}
        yesterday = str(datetime.date.today() + datetime.timedelta(days=-1))
        baidu_contents_sql = 'select content from baidu_search where game_name = "{0}" and get_time = "{1}";'.format(
                game_name, yesterday)
        server_cursor.execute(baidu_contents_sql)
        contents = [data["content"] for data in server_cursor.fetchall() if data["content"]]
        for i in range(len(contents)):
            content = re.sub('<span.*?>|#|</span>|\d?', "", contents[i])
            cut_sentence = jieba.cut(str(content), cut_all=False, HMM=True)
            for word in cut_sentence:
                if word not in user_stopwords:
                    if word not in array:
                        array[word] = 1
                    else:
                        array[word] = array[word] + 1
        sw = sorted(array.items(), key=lambda x: x[1], reverse=True)[:100]
        self.user_game_word_array[game_name] = {
            "count": len(contents),
            "top100": sw
        }

        server_cursor.close()
        print("{0}---down".format(game_name))

    def insert_today(self):
        server_db = self.local_db.get_instance()
        server_cursor = server_db.cursor(cursor=pymysql.cursors.DictCursor)
        yesterday = str(datetime.date.today() + datetime.timedelta(days=-1))
        #delete_sql = "delete from cut_today_word;"
        #server_cursor.execute(delete_sql)
        #server_db.commit()
        #print("清表完成{0}".format(delete_sql))
        for game_name in self.user_game_word_array:
            top100_info = self.user_game_word_array[game_name]
            count = top100_info["count"]
            top100 = top100_info["top100"]
            for one_tuple in top100:
                word = one_tuple[0]
                num = one_tuple[1]
                frequency = ('%.2f' % (num / count * 100)) + "%"
                insert_sql = "insert into cut_today_word(cut_word, word_frequency, today_word_num, today_total, username, project_name, game_name, uid, upload_time) " \
                             "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}', '{8}');".format(word, frequency, num, count, self.user_name, self.project_name, game_name, self.user_id, yesterday)
                try:
                    server_cursor.execute(insert_sql)
                    server_db.commit()
                    #print(insert_sql, ': success')
                except:
                    #server_db.rollback()
                    print(insert_sql, ':failed')

        server_db.commit()




    def main(self):
        server_db = self.local_db.get_instance()
        server_cursor = server_db.cursor(cursor=pymysql.cursors.DictCursor)
        username = self.user_name
        user_stopwords = self.get_stopwords(username=username)
        user_userdict = self.get_userdict(username=username)

        for game_name in self.game_array:
            game_name = game_name["game_name"]
            t = threading.Thread(target=self.start_cut, args=(username, game_name, user_stopwords, user_userdict))
            t.start()
            self.threads.append(t)
        for t in self.threads:
            t.join()

        self.insert_today()


        server_cursor.close()







if __name__ == '__main__':
    cut_word()
