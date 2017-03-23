from configuration import INGESTION_TOPIC, CONSUMER_GRP_MODEL1, GNDR_SERV_TOPIC, OUTPUT_COLNM
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
    cons = KfkConsumer(GNDR_SERV_TOPIC, CONSUMER_GRP_MODEL1)
    dbcon_oput = mongo_connect(col_nm=OUTPUT_COLNM)

    for msg in cons.consume():
        try:
            entry = prepare_output(msg)
            q = {'_id': entry['_id'], 'model': msg['model']}
            dbcon_oput.update(q, {'$set': entry}, upsert=True)
        except Exception as e:
            print(e)
            continue

if __name__ == '__main__':
    dispatch_output()
