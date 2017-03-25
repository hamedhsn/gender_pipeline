import logging
from flask import Flask
from flask_restful import reqparse, Api, Resource

from configuration import OUTPUT_COLNM
from utils.mongo import mongo_connect

app = Flask(__name__)
api = Api(app)

# set up request parser
parser = reqparse.RequestParser()


class WebServiceInfo(Resource):
    def get(self):
        return 'Webservice Version 1.0'


class GenderService(Resource):
    def get(self):

        args = parser.parse_args()
        parser.add_argument('cid', type=str)
        parser.add_argument('model', default='MODEL1', type=str)
        print(args)

        _id = '{}_{}'.format(args.cid, args.model)
        q = {'_id': _id}
        res = dbcon_oput.find_one(q)

        if res:
            return {res['cid']: res['gndr']}
        else:
            return 'The gender for provided client is not available!!'

# route resource here
api_base_url = '/api/v1'
api.add_resource(WebServiceInfo, '/')
api.add_resource(GenderService, api_base_url + '/gender')


# ######### EXAMPLES: #################
# curl 127.0.0.1:5000/api/v1/gender\?model=MODEL1\&\&cid=C123

if __name__ == '__main__':
    dbcon_oput = mongo_connect(col_nm=OUTPUT_COLNM)
    print(dbcon_oput)

    logging.info('Successfully loaded the app')
    app.run(host='0.0.0.0', debug=True)