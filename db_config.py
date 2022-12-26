import configparser
import os

exepath = os.getcwd()
exepath = os.path.join(exepath, "db_config.ini")

config = configparser.ConfigParser()
config.read(exepath, encoding='utf-8-sig')


class ReadConfig:
    @staticmethod
    def get_db(name):
        value = 0
        try:
            value = config.get('db', name)  # 通过config.get拿到配置文件中DATABASE的name的对应值
        except Exception as e:
            value = -1
        return value


if __name__ == '__main__':
    print('config_path', exepath)  # 打印输出config_path测试内容是否正确
    print('通过config.get拿到配置文件中DATABASE的对应值\n',
          'ip', ReadConfig().get_db('ip'),
          'port', ReadConfig().get_db('port'),
          'username', ReadConfig().get_db('username'),
          'password', ReadConfig().get_db('password'),
          'sqlpath', ReadConfig().get_db('sqlpath'),
          'tab_name', ReadConfig().get_db('tab_name'),
          'output_dir', ReadConfig().get_db('output_dir'),
          )
