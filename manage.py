import json
import os
from flask import Flask
from flask import Blueprint
from flask_cors import CORS
from flask_bcrypt import Bcrypt
from flask_caching import Cache
from flask_restplus import Api
from flask_script import Manager
from flask_restplus import Resource
from flask_restplus import Namespace
from flask_restplus.apidoc import apidoc

from app_config import app_env
from app_config import SERVER_NAME
from app_config import config_by_name
from app_config import cache_config_dict

from services import swm_netbins_fill_forecast
from services import swm_netbins_predominant_waste

URL_PREFIX = "/ndmc_swm_analytics_2x"
apidoc.url_prefix = URL_PREFIX
ndmc_swm_2x_dto = Namespace('ndmc-swm-Analytics-2x', description='NDMC SWM Analytics 2.x API')


flask_bcrypt = Bcrypt()
cache = Cache(config=cache_config_dict)
main_directory_path = os.path.dirname(os.path.abspath(__file__))


class localFlask(Flask):
    def process_response(self, response):
        # Every response will be processed here first
        response.headers['server'] = SERVER_NAME
        super(localFlask, self).process_response(response)
        return response


def create_app(config_name):
    app = localFlask(__name__)
    app.config['SWAGGER_UI_JSONEDITOR'] = True
    app.config['RESTPLUS_VALIDATE'] = True
    CORS(app, resources={r"/*": {"origins": '*'}})
    app.config.from_object(config_by_name[config_name])
    flask_bcrypt.init_app(app)
    cache.init_app(app)
    return app


blueprint = Blueprint('api', __name__, url_prefix=URL_PREFIX)

api = Api(blueprint, title='NDMC-Solid Waste Management 2.x Demo API Documentation',
          version='1.0', contact="Quantela", contact_url="https://quantela.com",
          contact_email="rajiv.v@quantela.com",
          description='The API documentation is for NDMC SWM Demo of Dashboards on 2.x Atlantis Platform')

api.add_namespace(ndmc_swm_2x_dto)

app = create_app(os.getenv('FLASK_APPLICATION_ENV') or 'dev')
app.register_blueprint(blueprint)
app.app_context().push()
manager = Manager(app)

@ndmc_swm_2x_dto.route('/binfill_forecast')
class EndPointFileApi(Resource):
    def get(self):
        response = swm_netbins_fill_forecast.main()
        return json.loads(response)


@ndmc_swm_2x_dto.route('/predominant_waste')
class EndPointFileApi(Resource):
    def get(self):
        response = swm_netbins_predominant_waste.main()
        return json.loads(response)




@manager.command
def run():
    env = app_env[os.getenv('FLASK_APPLICATION_ENV') or 'dev']
    app.run(host=env['host'], port=env['port'], debug=env['debug'])


if __name__ == '__main__':
    manager.run()
    # app.run(host="127.0.0.1", port="5000", debug=True)