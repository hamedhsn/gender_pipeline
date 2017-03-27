from genderservice.utils.kfkpywrapper import KfkConsumer
from genderservice import configuration as cfg
from genderservice.utils.mongo import mongo_connect


def prepare_output(msg):
    """ entry consumes from output topic

    :param msg: json entry should contains cid, gender, model name
    :return:
    """
    oput = {
        '_id': '{}_{}'.format(msg['cid'], msg['model']),
        'cid': msg['cid'],
        'gndr': msg['gndr'],
        'model': msg['model']
    }

    return oput


def dispatch_output():
    """ 1) consume the outputs from kafka output topics
        2) update the final collection for the front end service

    """
    cons = KfkConsumer(cfg.GNDR_SERV_TOPIC, cfg.CONSUMER_GRP_MODEL1)
    dbcon_oput = mongo_connect(col_nm=cfg.OUTPUT_COLNM)

    for msg in cons.consume():
        try:
            print(msg)
            # reform the output message
            entry = prepare_output(msg)

            # update _id and insert if it does not exists
            q = {'_id': entry['_id'], 'model': msg['model']}
            dbcon_oput.update(q, {'$set': entry}, upsert=True)

        except Exception as e:
            print('Unknown Error: {}'.format(e))
            continue

if __name__ == '__main__':
    dispatch_output()
