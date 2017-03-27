import argparse
import datetime

from genderservice import configuration as cfg
from genderservice.ops.modelbase import Model


class Model1(Model):
    def __init__(self, name=cfg.MODE1_NM, cons_grp=cfg.CONSUMER_GRP_MODEL1, col_nm=cfg.MODEL1_COLNM):
        super().__init__(consumer_grp=cons_grp, model_nm=name, col_nm=col_nm)

    def identify_gender(self, gndr_entry):
        """ Since solution is based on last gender visited nothing to do
        so simple return the gender entry

        :param gndr_entry:
        :return:
        """
        self.dbcon.insert(gndr_entry)

        return gndr_entry


class Model2(Model):
    def __init__(self, name=cfg.MODE2_NM, cons_grp=cfg.CONSUMER_GRP_MODEL2, col_nm=cfg.MODEL2_COLNM):
        super().__init__(consumer_grp=cons_grp, model_nm=name, col_nm=col_nm)

    @staticmethod
    def inc_gndr(counter_col, _id, gndr):
        """Increment the gender counter and return both counts

        :param gndr: gender
        :param counter_col: counter collection
        :param _id: the name of the field to be incremented
        :return: the next subsequent number
        """
        inc = dict()
        oth_gndr = cfg.GNDR_FEMALE if gndr == cfg.GNDR_MALE else cfg.GNDR_MALE

        inc['cnt.{}'.format(oth_gndr)] = 0
        inc['cnt.{}'.format(gndr)] = 1

        q = {'_id': _id}
        f = {'$inc': inc}
        res = counter_col.find_one_and_update(q, f, upsert=True)

        if not res:
            return {gndr: 1, oth_gndr: 0}
        else:
            res['cnt'][gndr] += 1
            return res['cnt']

    def identify_gender(self, gndr_entry):
        """ 1) Increment gender based on gender field in gender entry
            2) Compare count of each gender and return the selected gender

        :param gndr_entry: gender entry
        :return:
        """
        _id = gndr_entry['cid']
        gndr = gndr_entry['gndr']
        cnt = self.inc_gndr(self.dbcon, _id, gndr)

        gndr_entry['gndr'] = 'male' if cnt['male'] > cnt['female'] else 'female'
        return gndr_entry


class Model3(Model):
    def __init__(self, name=cfg.MODE3_NM, cons_grp=cfg.CONSUMER_GRP_MODEL3, col_nm=cfg.MODEL3_COLNM):
        super().__init__(consumer_grp=cons_grp, model_nm=name, col_nm=col_nm)

    def get_gendr(self, cid):
        """ 1) aggregate query to get sum of each genders
            2) return gender with max number

        :param cid: client id
        :return: output gender
        """
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
        """ 1) Insert gender entry
            2) call get_gendr to aggregate the genders and returns the max one

        :param gndr_entry: gender entry
        :return: target entry for output with the right gender
        """
        gndr_entry['createdAt'] = datetime.datetime.utcnow()
        self.dbcon.insert(gndr_entry)

        gndr_entry['gndr'] = self.get_gendr(gndr_entry['cid'])

        return gndr_entry

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Running model.')

    parser.add_argument('--model', default='MODEL3', dest='model',
                        help='Provide Model')

    args = parser.parse_args()

    if args.model == cfg.MODE1_NM:
        Model1().dispatch()

    elif args.model == cfg.MODE2_NM:
        Model2().dispatch()

    elif args.model == cfg.MODE3_NM:
        Model1().dispatch()
