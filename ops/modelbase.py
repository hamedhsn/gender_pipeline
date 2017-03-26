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
        """ consuming the kafka data / transform the ingested json into predefined template

        """
        for msg in self.cons.consume():

            gndr_entry = self.create_gndr_entry(msg, self.modelnm)

            yield gndr_entry

    def create_gndr_entry(self, msg, model_nm):
        """ create a json template that will be processed by identify_gender function

        :param msg: json ingested message
        :param model_nm: name of the model
        :return:
        """
        json_msg = {
            'cid': msg['cid'],
            'gndr': msg['gndr'],
            'model': model_nm
        }

        return json_msg

    def dispatch(self):
        """ a template method pattern that defines the program skeleton of the algorithm
        steps are:
        1) Consume the kafka
        2) Identify the gender by overridden identify_gender function in subclasses
        3) Produce the output result to kafka producer

        """
        for gndr_entry in self.consume():

            rslt_entry = self.identify_gender(gndr_entry)

            self.prod.produce(rslt_entry)

            print(rslt_entry)

    def identify_gender(self, gndr_entry):
        """ this function will implement gender identification in each
        child class that is inherited from this class

        :param gndr_entry: gender entry
        """
        raise NotImplementedError('Child class needs to implement this method.')
