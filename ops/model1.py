import datetime

from configuration import CONSUMER_GRP_MODEL1, MODEL1_COLNM, MODEL2_COLNM, \
    CONSUMER_GRP_MODEL2, MODEL3_COLNM, CONSUMER_GRP_MODEL3
from ops.model import Model
from utils.mongo import inc_gndr


class Model1(Model):
    def __init__(self, name='MODEL1', cons_grp=CONSUMER_GRP_MODEL1, col_nm=MODEL1_COLNM):
        super().__init__(consumer_grp=cons_grp, model_nm=name, col_nm=col_nm)

    def identify_gender(self, gndr_entry):
        return gndr_entry


class Model2(Model):
    def __init__(self, name='MODEL2', cons_grp=CONSUMER_GRP_MODEL2, col_nm=MODEL2_COLNM):
        super().__init__(consumer_grp=cons_grp, model_nm=name, col_nm=col_nm)

    def identify_gender(self, gndr_entry):
        _id = gndr_entry['cid']
        gndr = gndr_entry['gndr']
        cnt = inc_gndr(self.dbcon, _id, gndr)

        gndr_entry['gndr'] = 'male' if cnt['male'] > cnt['female'] else 'female'
        return gndr_entry


class Model3(Model):
    def __init__(self, name='MODEL3', cons_grp=CONSUMER_GRP_MODEL3, col_nm=MODEL3_COLNM):
        super().__init__(consumer_grp=cons_grp, model_nm=name, col_nm=col_nm)

    def get_gendr(self, cid):
        match = {'$match': {'cid': cid}}
        grp = {'$group': {'_id': '$gndr', 'tot': {'$sum': 1}}}
        res = self.dbcon.aggregate([match, grp])

        cnt_gndr = 0
        gndr = None
        for itm in res:
            if itm['tot'] > cnt_gndr:
                gndr = itm['_id']
                cnt_gndr = itm['tot']

        return gndr

    def identify_gender(self, gndr_entry):
        gndr_entry['createdAt'] = datetime.datetime.utcnow()
        self.dbcon.insert(gndr_entry)

        gndr_entry['gndr'] = self.get_gendr(gndr_entry['cid'])

        return gndr_entry

if __name__ == '__main__':
    Model1().dispatch()
