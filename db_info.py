from prettytable import PrettyTable

"""
V1.1.30.1
修改读取日志文件方式由之前的lines = lines + line改为list_tmp.append(line),增加部分时间戳
"""


def print_info():
    k = PrettyTable(field_names=["MySQL慢日志分析工具"])
    k.align["MySQL慢日志分析工具"] = "l"  # 以name字段左对齐
    k.padding_width = 1  # 填充宽度
    k.add_row(["Tool Version: 1.1.30.1"])
    k.add_row(["Powered By: DBA Group of Infrastructure Research Center"])
    print(k.get_string(sortby="MySQL慢日志分析工具", reversesort=False))


if __name__ == '__main__':
    print_info()
