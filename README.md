# MySQL慢日志解析工具

## 一、简介

通过正则解析MySQL慢日志文件，分析耗时的SQL以及分组汇总形成统计信息，生成慢查询报告

### 1.1 下载地址

https://github.com/iverycd/MysqlSlowLogAnalyze/releases

### 1.2 运行概览

- 慢查询汇聚以及TOP 20慢SQL

![](images/16720334424193/16720347864814.jpg)

- 所有慢SQL执行情况

![](images/16720334424193/16720342680528.jpg)

- SQL散点分布

![](images/16720334424193/16720345809807.jpg)




## 二、使用
### Windows
1、解压压缩包到任意目录

2、将MySQL的慢日志放到slow_log目录

![](images/16720334424193/16720341412361.jpg)

3、运行run.bat

![](images/16720334424193/16720341754333.jpg)

### MacOS
1、解压压缩包到任意目录

2、将MySQL的慢日志放到slow_log目录

![](images/16720334424193/16720364573066.jpg)

3、终端运行

```bash

./MySlowLogParse

```
## 三、查看报告

报告默认会在当前解压目录生成

![](images/16720334424193/16720340743210.jpg)

