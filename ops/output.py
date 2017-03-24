# from configuration import CONSUMER_GRP_MODEL1, GNDR_SERV_TOPIC, OUTPUT_COLNM
import configuration as cfg
from utils.kfkpywrapper import KfkConsumer
from utils.mongo import mongo_connect


def prepare_output(msg):
    oput = {
        '_id': '{}_{}'.format(msg['cid'], msg['model']),
        'cid': msg['cid'],
        'gndr': msg['gndr'],
        'model': msg['model']
    }

    return oput


def dispatch_output():
    cons = KfkConsumer(cfg.GNDR_SERV_TOPIC, cfg.CONSUMER_GRP_MODEL1)
    dbcon_oput = mongo_connect(col_nm=cfg.OUTPUT_COLNM)

    for msg in cons.consume():
        try:
            entry = prepare_output(msg)
            print(entry)
            q = {'_id': entry['_id'], 'model': msg['model']}
            dbcon_oput.update(q, {'$set': entry}, upsert=True)
        except Exception as e:
            print(e)
            continue

if __name__ == '__main__':
    dispatch_output()
