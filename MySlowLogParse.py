import datetime
import hashlib
import os
import pathlib
import platform
import re
import sys
import time
from db_config import ReadConfig
import db_excel
import db_charts
from db_info import print_info
import pandas as pd
from pandasql import sqldf

pat = """^# Time: (?P<time>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}\+\d{2}:\d{2}\s?)
# User@Host: (?P<User_Host>.*?)\s?
# Query_time: (?P<Query_time>\d+\.\d+)\s+Lock_time: (?P<Lock_time>\d+\.\d+)\s+Rows_sent: (?P<Rows_sent>\d+)\s+Rows_examined: (?P<Rows_examined>\d+)\s?
(?P<Query>[^#]+)#?$"""

list_parse_log = []  # 给dataframe用的list，已经对慢日志的各个列进行正确处理


# 统计文件夹下的文件个数
def show_file_tree(file_path):
    # 获取当前目录下的文件列表
    file_list = os.listdir(file_path)
    file_count = 0
    folder_count = 0
    # 遍历文件列表，如果当前文件不是文件夹，则文件数量+1，如果是文件夹，则文件夹数量+1且再调用统计文件个数的方法
    for i in file_list:
        path_now = file_path + "\\" + i
        if os.path.isdir(path_now):
            folder_count = folder_count + 1
            show_file_tree(path_now)
        else:
            file_count = file_count + 1
    return file_count


class DbLog(object):
    def __init__(self):
        ini_file = ReadConfig()
        self.sqlpath = ini_file.get_db('sqlpath')
        self.output_dir = ini_file.get_db('output_dir') + '/'

    def read_log_path(self, sql_log_path):
        ret = 0
        memory_df = ''
        if sql_log_path != '':
            fileNum = show_file_tree(sql_log_path)
            if fileNum == 0:
                return 0
            # 获取慢日志目录下的所有文件名
            file_list = os.listdir(sql_log_path)
            for v_out in file_list:
                if platform.system().upper() == 'WINDOWS':
                    logfile_abs_name = sql_log_path + '\\' + v_out
                else:
                    logfile_abs_name = sql_log_path + '/' + v_out
                # 获取文件的后缀,用来判断是否是日志文件以及是否是目录，不是log为后缀名的一律忽略掉
                file_houzhui = pathlib.Path(v_out).suffix
                if file_houzhui == '.log':
                    print(datetime.datetime.now(), "----------------", "开始分析慢日志文件：" + v_out)
                    # 开始使用read_log_lines分析日志每一行
                    ret, memory_df = self.read_log_lines(logfile_abs_name)
                else:
                    print("----------------忽略非SQL日志文件：" + v_out + "-------------")
        else:
            print(datetime.datetime.now(), "----------------", '获取慢日志文件路径异常，请检查配置文件！')
            return 0
        return ret, memory_df

    def read_log_lines(self, filename):
        slow_result = []  # 正则匹配慢日志，获取到正确的一行行信息
        lines = ''  # 慢日志原始信息
        list_tmp = []
        read_count = 0  # 慢日志读取的行数
        print(datetime.datetime.now(), "----------------", '开始读取文件')
        with open(filename, "r", encoding='utf-8', errors='ignore') as f:
            for line in f.readlines():
                # lines = lines + line
                list_tmp.append(line)
                read_count += 1
                if read_count % 100000 == 0:
                    print(datetime.datetime.now(), "----------------", '已处理行数：', read_count)
        lines = ''.join(list_tmp)
        print(datetime.datetime.now(), "----------------", '读取文件完毕')
        headers_printed = False
        print(datetime.datetime.now(), "----------------", '正在分析数据，请稍等')
        for match in re.finditer(pat, lines, re.MULTILINE | re.DOTALL):
            if not headers_printed:
                headers_printed = True
            slow_result.append(list(match.groups()))
        for slow_item in slow_result:
            sql_finish_time = re.sub(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).(\d{6})\+(\d{2}):(\d{2})',
                                     r'\1-\2-\3 \4:\5:\6.\7', slow_item[0])
            exec_client = slow_item[1]
            query_time = slow_item[2]
            lock_time = slow_item[3]
            rows_sent = slow_item[4]
            rows_examined = slow_item[5]
            sql_text = slow_item[6]
            sql_text_qudiao_huanhang = sql_text.replace('\n', '')
            pattern = re.compile(r'SET timestamp=\d{10};')
            time_str_list = pattern.findall(sql_text_qudiao_huanhang)
            time_str = time_str_list[0].replace('SET timestamp=', '')
            sql_start_pos = sql_text_qudiao_huanhang.find(time_str) + len(time_str)
            sql_text = sql_text_qudiao_huanhang[sql_start_pos:]
            # print(sql_text)
            if re.match(r'/*.*\*/\s', sql_text):  # 先检测是否包含注释行/* */
                str_len = len(re.findall(r'/*.*\*/\s', sql_text)[0])  # 获取注释行开头字符串的长度
                optype = re.split(r'\s+', sql_text[str_len:])[0].upper()  # 截取SQL操作类型
            else:
                optype = re.split(r'\s+', sql_text)[0].upper()  # 空格作为分割截取SQL操作类型
            query = sql_text.strip().lower()  # 去除首尾多余的空格以及字符串转为小写
            # query = sql_text.rstrip(';')  # 去除结尾的分号
            query = re.sub(r' +', ' ', query)  # 多个空格格式化为1个空格
            query = re.sub(r'/*.*\*/\s', "", query)  # 去除注释，即/* */的注释内容
            query = re.sub(r'\\["\']', "", query)  # 去掉SQL语句条件文本内容里，包含的单引号或者双引号
            query = re.sub(r'".*?"', "?", query)  # 把双引号包围的字符串内容连同双引号一起替换为问号
            query = re.sub(r'\'.*?\'', "?", query)  # 把单引号包围的字符串内容连同双引号一起替换为问号
            query = re.sub(r'\bfalse\b|\btrue\b', "?", query, flags=re.I)  # 不区分大小写的true或者false替换为问号
            query = re.sub(r'[0-9+-][0-9a-f.xb+-]*', "?", query)  # where条件里的数字替换为问号
            query = re.sub(r'\b[0-9+-][0-9a-f.xb+-]*', "?", query)  # where条件里的数字替换为问号
            query = re.sub(r'\bnull\b', "?", query, flags=re.I)  # where里的null替换为问号
            query = re.sub(r'\b(in|values?)(?:[\s,]*\([\s?,]*\))+', r'\1(?+)', query,
                           flags=re.I)  # 将in,values匹配的多个问号替换为"?+"
            query = re.sub(r'\blimit \?(?:, ?\?| offset \?)?', ' limit ?', query,
                           flags=re.I)  # limit offset 统一格式化为limit ?
            if re.match(r"\b(select\s.*?)(?:(\sunion(?:\sall)?)\s)+", query, flags=re.I):  # 最后一个union all使用repeat注释
                query = re.sub(r"\b(select\s.*?)(?:(\sunion(?:\sall)?)\s\1)+", r'\1 /*repeat\2*/', query, flags=re.I)
            # sql文本如果带有order by asc条件，去掉order by里的asc这个冗余的字符
            if re.match(r'(.*) order\s+by (.*?)\s+asc', query, flags=re.I):
                query = re.sub(r'\s+ASC', '', query, flags=re.I)
            query = re.sub(r"\A\s*call\s+(\S+)\(.*?\).*;", r'call \1', query, flags=re.I)  # call形式的存储过程，去掉括号
            # print(query)  # 去掉参数之后格式化的sql形式
            md5_query = hashlib.md5(query.encode()).hexdigest().upper()  # 计算文本字符串转为md5的十六进制字符串
            # print(md5_query)
            # 以下将慢日志各信息存到list。以便组成dataframe
            list_parse_log.append(
                [sql_finish_time, exec_client, query_time, lock_time, rows_sent, rows_examined, sql_text, query, optype,
                 md5_query])
        df = pd.DataFrame(list_parse_log,
                          columns=['sql_finish_time', 'exec_client', 'query_time', 'lock_time', 'rows_sent',
                                   'rows_examined', 'sql_text', 'query', 'optype', 'md5_query'])
        return read_count, df

    def query_report(self, df, dest_name):
        query_num = 0
        df_rows = len(df)
        list_col_name = ['query_time', 'lock_time', 'rows_sent', 'rows_examined']
        sql_path = self.sqlpath
        qps, conc = 0, 0
        # 以下是总体汇总情况
        sql_duration_sec = (
                pd.to_datetime(df['sql_finish_time'], format="%Y/%m/%d", errors='coerce').max() - pd.to_datetime(
            df['sql_finish_time'], format="%Y/%m/%d", errors='coerce').min()).total_seconds()
        if sql_duration_sec > 0:
            qps = '{:.2f}'.format(df_rows / sql_duration_sec)
            conc = '{:.4f}'.format((df['query_time'].astype(float).sum()) / sql_duration_sec)
        if platform.system().upper() == 'WINDOWS':
            file_name = dest_name + '\\slow_rpt.txt'
        else:
            file_name = dest_name + '/slow_rpt.txt'
        rpt = open(file_name, 'a+', encoding='utf-8', errors='ignore')
        rpt.write('Current date: ' + str(datetime.datetime.now()) + '\n')
        rpt.write('# Hostname: ' + str(platform.node()) + '\n')
        rpt.write('# Files Directory: ' + sql_path + '\n')
        rpt.write('# Overall: ' + str(len(df)) + ' total,' + str(len(df.groupby('md5_query'))) + ' unique, ' + str(
            qps) + ' QPS, ' + str(conc) + ' concurrency\n')
        rpt.write('# Time range: ' + str(
            pd.to_datetime(df['sql_finish_time'], format="%Y/%m/%d", errors='coerce').min()) + ' to ' + str(
            pd.to_datetime(df['sql_finish_time'], format="%Y/%m/%d", errors='coerce').max()) + '\n')
        rpt.write('# Attribute' + 'total'.rjust(23) + 'min'.rjust(15) + 'max'.rjust(15) + 'avg'.rjust(15) + '95%'.rjust(
            15) + 'stddev'.rjust(15) + 'median'.rjust(15) + '\n')
        rpt.write(
            '# =================' + '======='.rjust(15) + '======='.rjust(15) + '======='.rjust(15) + '======='.rjust(
                15) + '======='.rjust(15) + '======='.rjust(15) + '======='.rjust(15) + '\n')
        for items in ['query_time', 'lock_time', 'rows_sent', 'rows_examined', 'sql_text']:
            if items not in 'sql_text':
                attribute = items
                if items == 'query_time':
                    attribute = 'exec_time_seconds'
                if items == 'lock_time':
                    attribute = 'lock_time_seconds'
                if items == 'rows_sent':
                    attribute = 'rows_sent__counts'
                if items == 'rows_examined':
                    attribute = 'row_examine_count'
                rpt.write('#' + attribute + '{:.0f}'.format(df[items].astype(float).sum()).rjust(16) +
                          '{:.2f}'.format(df[items].astype(float).min()).rjust(15) +
                          '{:.2f}'.format(df[items].astype(float).max()).rjust(15) +
                          '{:.2f}'.format(df[items].astype(float).mean()).rjust(15) +
                          '{:.2f}'.format(df[items].astype(float).quantile(0.95)).rjust(15) +
                          '{:.2f}'.format(df[items].astype(float).std()).rjust(15) +
                          '{:.2f}'.format(df[items].astype(float).quantile()).rjust(15) + '\n'
                          )
            else:
                rpt.write('#' + 'sql__char__length' + '{:.0f}'.format(df['sql_text'].str.len().sum()).rjust(16) +  # 字符数
                          '{:.2f}'.format(df['sql_text'].str.len().min()).rjust(15) +
                          '{:.2f}'.format(df['sql_text'].str.len().max()).rjust(15) +
                          '{:.2f}'.format(df['sql_text'].str.len().mean()).rjust(15) +
                          '{:.2f}'.format(df['sql_text'].str.len().quantile(0.95)).rjust(15) +
                          '{:.2f}'.format(df['sql_text'].str.len().std()).rjust(15) +
                          '{:.2f}'.format(df['sql_text'].str.len().quantile()).rjust(15) + '\n'
                          )
        rpt.write('\n# Profile\n')
        rpt.write(
            '# Rank ' + ' Query ID '.rjust(1) + ' Response time '.rjust(41) + ' Calls ' + ' R/Call ' + ' SQL_TEXT \n')
        query = """select ROW_NUMBER() over (order by sum(query_time) desc) rank,md5_query Query_ID,sum(query_time) Response_time,sum(query_time)/(select sum(query_time) from df) percent,count(md5_query) Calls,avg(query_time) avg_query_time,substr(sql_text,1,2000) sql_text
from df group by md5_query limit 20"""
        df_sql = sqldf(query)
        all_data = df_sql.apply(lambda x: tuple(x), axis=1).values.tolist()  # 每一行所有数据以列表list输出
        for v_all_data in all_data:
            rank = str(v_all_data[0])
            query_id = str(v_all_data[1])
            response_time = str('{:.4f}'.format(v_all_data[2]))
            percent = str('{:.1f}'.format(v_all_data[3]) + '%')
            calls = str(v_all_data[4])
            avg_query_time = str('{:.4f}'.format(v_all_data[5]))
            sql_text = str(v_all_data[6])
            sql_text = re.sub(r' +', ' ', sql_text).strip()[0:70]
            rpt.write(
                rank.rjust(6) + '  ' + query_id.rjust(8) + response_time.rjust(11) + percent.rjust(6) + calls.rjust(7) +
                avg_query_time.rjust(8) + '  ' + sql_text.rjust(10) + '\n')
        # 以下是每个SQL的详细情况
        query = """select max(md5_query) from df group by md5_query order by sum(query_time) desc limit 20"""
        df_sql = sqldf(query)
        detail_sql = df_sql.values
        for sql_md5_str in detail_sql:
            sql_md5_str = sql_md5_str[0]
            query_num += 1
            rpt.write('\n# Query ' + str(query_num) + ' ,ID ' + sql_md5_str + '\n')
            rpt.write('# Time range: ' + str(df[df.md5_query.isin([sql_md5_str])]['sql_finish_time'].min()) + ' to ' +
                      str(df[df.md5_query.isin([sql_md5_str])]['sql_finish_time'].max()) + '\n')
            rpt.write('# Attribute          pct            total               min                max                avg                95%             stddev\n')
            rpt.write('# ============       ===            =====              ====                ===                ===                ===              =====\n')
            rpt.write('#        Count ' + '{:.0f}'.format(len(df[df.md5_query.isin([sql_md5_str])])/df_rows).rjust(9) + ' '.rjust(5) + str(len(df[df.md5_query.isin([sql_md5_str])])).rjust(12) + '\n')
            for col_name in list_col_name:
                if col_name == 'query_time':
                    out_col_name = 'Exec time'
                elif col_name == 'lock_time':
                    out_col_name = 'Lock time'
                elif col_name == 'rows_sent':
                    out_col_name = 'Rows sent'
                elif col_name == 'rows_examined':
                    out_col_name = 'examined'
                else:
                    out_col_name = ''
                col_sum = df[df.md5_query.isin([sql_md5_str])][col_name].astype(float).sum()
                col_min = '{:.2f}'.format(df[df.md5_query.isin([sql_md5_str])][col_name].astype(float).min())
                col_max = '{:.2f}'.format(df[df.md5_query.isin([sql_md5_str])][col_name].astype(float).max())
                col_avg = '{:.2f}'.format(df[df.md5_query.isin([sql_md5_str])][col_name].astype(float).mean())
                col_per95 = '{:.2f}'.format(df[df.md5_query.isin([sql_md5_str])][col_name].astype(float).quantile(0.95))
                col_std = '{:.2f}'.format(df[df.md5_query.isin([sql_md5_str])][col_name].astype(float).std())
                col_percent = '{:.2f}'.format(col_sum / df[col_name].astype(float).sum())
                rpt.write(
                    '#  ' + out_col_name.rjust(11) + '     ' + col_percent.rjust(5) + '  ' + '{:.1f}'.format(col_sum).rjust(15) + '   ' + col_min.rjust(15) + '    ' + col_max.rjust(15) + '    ' + col_avg.rjust(15) + '    ' + col_per95.rjust(15) + '    ' +
                    col_std.rjust(15) + '\n')
            rpt.write(df[df.md5_query.isin([sql_md5_str])]['sql_text'].min() + '\n')
        rpt.close()

    def query_max_exec_time(self, df):
        # 分组汇总输出sql执行情况按最大执行耗时排序
        query = """select row_number() over (order by max_tim desc) 序号,cast(s as varchar) SQL文本,max_tim 最大执行耗时_秒,min_tim 最小执行耗时_秒,avg_90 百分之90平均执行耗时_秒,avg_tim 平均执行耗时_秒,cnt 执行次数,max_rcount 行数,lock_time 等待时间_秒 from
        (select s,max(tim)over(partition by s) max_tim,min(tim)over(partition by s) min_tim,avg(tim)over(partition by s) avg_tim,
        avg(case when tile<=9 then tim else null end)over(partition by s) avg_90,
        count(1)over(partition by s) cnt,
        row_number()over(partition by s order by tim desc) r ,
        max(rows_examined)over(partition by s) max_rcount,
        lock_time  from (select sql_text s,cast(query_time as numeric)  tim,ntile(10) over(partition by sql_text order by query_time) tile,cast(rows_examined as numeric) rows_examined,cast(lock_time as numeric) lock_time from  df  where  query_time >=  0 ) t1)  xx
         where r = 1"""
        df_sql = sqldf(query)
        fields = ['序号', 'sql文本', '最大执行耗时_秒', '最小执行耗时_秒', '百分之90平均执行耗时_秒', '平均执行耗时_秒', '执行次数', '行数', '等待时间_秒']  # 获取所有字段名
        all_data = df_sql.apply(lambda x: tuple(x), axis=1).values.tolist()  # 每一行转为元组tuple，所有数据以列表list输出
        excel_rows_num = len(all_data) + 1
        return all_data, fields, excel_rows_num

    def query_max_exec_count(self, df):
        # 分组汇总输出sql执行情况按最大执行次数排序
        query = """select row_number() over (order by cnt desc) 序号,cast(s as varchar) SQL文本,max_tim 最大执行耗时_秒,min_tim 最小执行耗时_秒,avg_90 百分之90平均执行耗时_秒,avg_tim 平均执行耗时_秒,cnt 执行次数,max_rcount 行数,lock_time 等待时间_秒 from     (select s , max(tim)over(partition by s) max_tim,min(tim)over(partition by s) min_tim,avg(tim)over(partition by s) avg_tim, avg(case when tile<=9 then tim else null end)over(partition by s) avg_90,
                       count(1)over(partition by s) cnt,row_number()over(partition by s order by tim desc) r ,
                       max(rows_examined)over(partition by s) max_rcount,
                       lock_time from (select sql_text s,cast(query_time as numeric) tim,ntile(10) over(partition by sql_text order by query_time) tile,cast(rows_examined as numeric) rows_examined,cast(lock_time as numeric) lock_time from df  where  query_time >=  0) t1 ) xx where r = 1 and cnt>=0"""
        df_sql = sqldf(query)
        fields = ['序号', 'sql文本', '最大执行耗时_秒', '最小执行耗时_秒', '百分之90平均执行耗时_秒', '平均执行耗时_秒', '执行次数', '行数', '等待时间_秒']  # 获取所有字段名
        all_data = df_sql.apply(lambda x: tuple(x), axis=1).values.tolist()  # 所有数据
        excel_rows_num = len(all_data) + 1
        # print(excel_rows_num)
        return all_data, fields, excel_rows_num

    def query_all_data(self, df):
        query = """select row_number() over () as rownum,sql_finish_time,query_time*1000 query_time,exec_client,substr(replace(replace(cast(sql_text as varchar),char(10),''),char(13),''),1,200), optype from df"""
        df_sql = sqldf(query)
        # all_data = df_sql.apply(lambda x: tuple(x), axis=1).values.tolist()
        df_data_list = df_sql.values.tolist()
        return df_data_list


class Logger(object):
    def __init__(self, filename='run.log', add_flag=True,
                 stream=open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)):
        self.terminal = stream
        self.filename = filename
        self.add_flag = add_flag

    def write(self, message):
        if self.add_flag:
            with open(self.filename, 'a+', encoding='utf-8') as log:
                try:
                    self.terminal.write(message)
                    log.write(message)
                except Exception as e:
                    print(e)
        else:
            with open(self.filename, 'w', encoding='utf-8') as log:
                try:
                    self.terminal.write(message)
                    log.write(message)
                except Exception as e:
                    print(e)

    def flush(self):
        pass


def main():
    print_info()
    time_start = time.time()
    hour = time.localtime().tm_hour
    minute = time.localtime().tm_min
    ss = time.localtime().tm_sec
    dbLog = DbLog()
    path = dbLog.sqlpath
    output_result_dir = dbLog.output_dir
    if len(output_result_dir) <= 0:
        output_result_dir = '.'
    destDirName = str(time.strftime("%Y_%m_%d"))
    rpt_dir_name = "REPORT_LOG_" + destDirName + "_" + str(hour) + "_" + str(minute) + "_" + str(ss)
    if platform.system().upper() == 'WINDOWS':
        destDirName = output_result_dir + "\\" + rpt_dir_name
        excel_dest = destDirName + "\\mysql_slow_sql.xlsx"
        run_log_dir = '\\run_info.log'
        excel_file = "\\mysql_slow_sql.xlsx"
        html_file = "\\mysql_slow_sql_charts.html"
        rpt_dir = os.path.dirname(os.path.realpath(excel_dest))
    else:
        destDirName = output_result_dir + rpt_dir_name
        excel_dest = destDirName + "mysql_slow_sql.xlsx"
        run_log_dir = '/run_info.log'
        excel_file = "/mysql_slow_sql.xlsx"
        html_file = "/mysql_slow_sql_charts.html"
        rpt_dir = os.path.dirname(os.path.realpath(excel_dest)) + '/' + rpt_dir_name
    sys.stdout = Logger(destDirName + run_log_dir, True, sys.stdout)
    if not os.path.exists(destDirName):
        os.makedirs(destDirName)
        print(datetime.datetime.now(), "----------------", "创建输出结果目录", destDirName, "成功！")
    else:
        print("输出结果目录" + destDirName + "已存在")
    ret, out_df = dbLog.read_log_path(path)
    if ret != 0:
        # 生成按执行时间维度统计结果
        sql_result_list = dbLog.query_max_exec_time(out_df)
        # 生成按执行次数分析的统计结果
        sql_result_list2 = dbLog.query_max_exec_count(out_df)
        # 生成SQL散点图之前获取数据
        sql_result_list3 = dbLog.query_all_data(out_df)
        dbLog.query_report(out_df, destDirName)
        if len(sql_result_list[0]) > 0 and len(sql_result_list2[0]) > 0:
            # 将以上2个结果写入到同一个excel不同的sheet
            print(datetime.datetime.now(), "----------------", '正在生成Excel')
            db_excel.read_db_to_xlsx(sql_result_list, sql_result_list2, destDirName + excel_file)
            print(datetime.datetime.now(), "----------------", '生成Excel完毕')
            # 生成SQL散点图
            print(datetime.datetime.now(), "----------------", '正在生成散点图')
            db_charts.scatter_plots(sql_result_list3, destDirName + html_file)
            time_end = time.time()
            print(datetime.datetime.now(), "----------------", '程序解析共耗时：', round(time_end - time_start, 2), '秒')
            print("解析慢日志完毕，请查看慢日志报告", rpt_dir)
        else:
            print('没有查询到慢日志数据，生成Excel以及散点图失败，请检查日志文件！')
    else:
        print('慢日志解析失败！')


if __name__ == "__main__":
    main()
