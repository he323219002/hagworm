# -*- coding: utf-8 -*-

from hagworm.extend.struct import Configure


class _Config(Configure):

    def _init_options(self):

        ##################################################
        # 基本

        self.Port = self._parser.getint(r'Base', r'Port')

        self.Debug = self._parser.getboolean(r'Base', r'Debug')

        self.GZip = self._parser.getboolean(r'Base', r'GZip')

        self.Secret = self._parser.get(r'Base', r'Secret')

        ##################################################
        # 日志

        self.LogLevel = self._parser.get(r'Log', r'LogLevel')

        self.LogFilePath = self._parser.get(r'Log', r'LogFilePath')

        self.LogFileBackups = self._parser.getint(r'Log', r'LogFileBackups')

        ##################################################
        # 时间

        self.SessionExpire = self._parser.getint(r'Time', r'SessionExpire')

        self.FuncCacheExpire = self._parser.getint(
            r'Time', r'FuncCacheExpire')

        self.LocalCacheExpire = self._parser.getint(
            r'Time', r'LocalCacheExpire')

        self.FileCacheExpire = self._parser.getint(
            r'Time', r'FileCacheExpire')

        ##################################################
        # 线程/进程

        self.ThreadPoolMaxWorkers = self._parser.getint(
            r'Pool', r'ThreadPoolMaxWorkers')

        self.ProcessPoolMaxWorkers = self._parser.getint(
            r'Pool', r'ProcessPoolMaxWorkers')

        ##################################################
        # MySql数据库

        self.MySqlMasterServer = self._parser.get_split_host(
            r'MySql',
            r'MySqlMasterServer'
        )

        self.MySqlSlaveServer = self._parser.get_split_host(
            r'MySql',
            r'MySqlSlaveServer'
        )

        self.MySqlName = self._parser.get(r'MySql', r'MySqlName')

        self.MySqlUser = self._parser.get(r'MySql', r'MySqlUser')

        self.MySqlPasswd = self._parser.get(r'MySql', r'MySqlPasswd')

        self.MySqlMasterMinConn = self._parser.getint(
            r'MySql',
            r'MySqlMasterMinConn'
        )

        self.MySqlMasterMaxConn = self._parser.getint(
            r'MySql',
            r'MySqlMasterMaxConn'
        )

        self.MySqlSlaveMinConn = self._parser.getint(
            r'MySql',
            r'MySqlSlaveMinConn'
        )

        self.MySqlSlaveMaxConn = self._parser.getint(
            r'MySql',
            r'MySqlSlaveMaxConn'
        )

        ##################################################
        # Mongo数据库

        self.MongoHost = self._parser.get_split_str(r'Mongo', r'MongoHost')

        self.MongoName = self._parser.get(r'Mongo', r'MongoName')

        self.MongoUser = self._parser.get(r'Mongo', r'MongoUser')

        self.MongoPasswd = self._parser.get(r'Mongo', r'MongoPasswd')

        self.MongoMinConn = self._parser.getint(r'Mongo', r'MongoMinConn')

        self.MongoMaxConn = self._parser.getint(r'Mongo', r'MongoMaxConn')

        ##################################################
        # 缓存

        self.RedisHost = self._parser.get_split_host(r'Redis', r'RedisHost')

        self.RedisBase = self._parser.getint(r'Redis', r'RedisBase')

        self.RedisPasswd = self._parser.getstr(r'Redis', r'RedisPasswd')

        self.RedisMinConn = self._parser.getint(r'Redis', r'RedisMinConn')

        self.RedisMaxConn = self._parser.getint(r'Redis', r'RedisMaxConn')

        self.RedisExpire = self._parser.getint(r'Redis', r'RedisExpire')

        self.RedisKeyPrefix = self._parser.get(r'Redis', r'RedisKeyPrefix')

        ##################################################
        # 事件总线

        self.SystemEventBusChannels = self._parser.getint(
            r'Event', r'SystemEventBusChannels')

        ##################################################
        # HTTP访问

        self.HttpErrorAutoRetry = self._parser.getint(
            r'Http', r'HttpErrorAutoRetry')

        self.HttpConnectTimeout = self._parser.getfloat(
            r'Http', r'HttpConnectTimeout')

        self.HttpRequestTimeout = self._parser.getfloat(
            r'Http', r'HttpRequestTimeout')

        ##################################################
        # 测试配置

        self.UnitTestEnable = self._parser.getboolean(
            r'Test', r'UnitTestEnable')

        self.AsyncTestTimeout = self._parser.getint(
            r'Test', r'AsyncTestTimeout')

        self.TestServerHost = self._parser.get(r'Test', r'TestServerHost')

        ##################################################
        # 支付宝

        self.AlipayRSAPrivateKey = self._parser.get(
            r'Alipay', r'AlipayRSAPrivateKey')

        self.AlipayPartnerID = self._parser.get(r'Alipay', r'AlipayPartnerID')

        self.AlipayRSAPublicKey = self._parser.get(
            r'Alipay', r'AlipayRSAPublicKey')

        self.AlipayAppID = self._parser.get(r'Alipay', r'AlipayAppID')

        self.AlipayAppRSAPublicKey = self._parser.get(
            r'Alipay', r'AlipayAppRSAPublicKey')

        ##################################################
        # 阿里云

        self.AliyunAccessKey = self._parser.get(r'Aliyun', r'AliyunAccessKey')

        self.AliyunSecretKey = self._parser.get(r'Aliyun', r'AliyunSecretKey')

        self.AliyunOssEndpoint = self._parser.get(
            r'Aliyun', r'AliyunOssEndpoint')

        self.AliyunOssBucket = self._parser.get(r'Aliyun', r'AliyunOssBucket')

        self.AliyunSmsEndpoint = self._parser.get(
            r'Aliyun', r'AliyunSmsEndpoint')

        ##################################################
        # 微信公众

        self.WeixinAppID = self._parser.get(r'Weixin', r'WeixinAppID')

        self.WeixinAppSecret = self._parser.get(r'Weixin', r'WeixinAppSecret')

        self.WeixinAppToken = self._parser.get(r'Weixin', r'WeixinAppToken')

        self.WeixinPayID = self._parser.get(r'Weixin', r'WeixinPayID')

        self.WeixinPaySecret = self._parser.get(r'Weixin', r'WeixinPaySecret')

        ##################################################
        # 第三方接口

        # 百度地图

        self.BaiduMapAK = self._parser.get(r'Third', r'BaiduMapAK')

        # 图灵机器人

        self.TuringApiKey = self._parser.get(r'Third', r'TuringApiKey')


config = _Config()
