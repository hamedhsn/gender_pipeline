from configuration import GNDR_SERV_TOPIC
from configuration import INGESTION_TOPIC
from utils.kfkpywrapper import KfkConsumer, KfkProducer
from utils.mongo import mongo_connect


class Model:
    def __init__(self, consumer_grp, model_nm, col_nm):
        self.cons = KfkConsumer(INGESTION_TOPIC, consumer_grp)
        self.prod = KfkProducer(GNDR_SERV_TOPIC)
        self.dbcon = mongo_connect(col_nm=col_nm)

        self.modelnm = model_nm

    def consume(self):
        for msg in self.cons.consume():
            gndr_entry = self.create_gndr_entry(msg, self.modelnm)
            yield gndr_entry

    def create_gndr_entry(self, msg, model):
        cid = msg['cid']
        gndr = msg['gndr']

        json_msg = {
            'cid': cid,
            'gndr': gndr,
            'model': model
        }

        return json_msg

    def dispatch(self):
        for gndr_entry in self.consume():
            rslt_entry = self.identify_gender(gndr_entry)

            self.prod.produce(rslt_entry)
    # def dispatch(self):
        # consume message/find gender based on model/push to kafka topics
        # raise NotImplementedError('Non-abstract classes need to implement this method.')

    def identify_gender(self, gndr_entry):
        # consume message/find gender based on model/push to kafka topics
        raise NotImplementedError('Non-abstract classes need to implement this method.')
