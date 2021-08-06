import os

SERVER_NAME = 'RE Dev Server'

app_env = {
    'local': {
        'host': 'localhost',
        'port': 5000,
        'debug': True
    },
    'dev': {
        'host': 'localhost',
        'port': 9002,
        'debug': True
    },
    'qa': {
        'host': '0.0.0.0',
        'port': 5000,
        'debug': False
    }

}

cache_config_dict = {
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_HOST': '0.0.0.0',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_DEFAULT_TIMEOUT': 3600
}

class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'the_secret_key')
    DEBUG = False


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    DEBUG = True
    TESTING = True


class LocalDevelopmentConfig(Config):
    DEBUG = False


class ProductionConfig(Config):
    DEBUG = False


class QAConfig(Config):
    DEBUG = False


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig,
    local=LocalDevelopmentConfig,
    qa=QAConfig
)

key = Config.SECRET_KEY