from random import randint

from configuration import INGESTION_TOPIC
from utils.kfkpywrapper import KfkProducer


def ingest():
    """ ingest random client id and gender type

    """
    prod = KfkProducer(INGESTION_TOPIC)

    for i in range(50000):
        cid = 'C{}'.format(randint(1, 200))
        gndr = 'male' if randint(0, 1) else 'female'

        json_msg = {
            'cid': cid,
            'gndr': gndr
        }

        # Produce json input metric
        prod.produce(json_msg)


if __name__ == '__main__':
    ingest()
