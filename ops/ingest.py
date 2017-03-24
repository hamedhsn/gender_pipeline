from configuration import INGESTION_TOPIC
from utils.kfkpywrapper import KfkProducer


def ingest(json_msg):
    prod.produce(json_msg)


if __name__ == '__main__':
    prod = KfkProducer(INGESTION_TOPIC)

    for i in range(5):
        json_msg = {
            'cid': 'C128',
            'gndr': 'female'
        }
        print('produce : {}'.format(json_msg))
        ingest(json_msg)
