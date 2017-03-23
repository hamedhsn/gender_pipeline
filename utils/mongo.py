from configuration import MNG_DBNAME, MNG_HOST, MNG_PORT, MNG_USER_NAME, MNG_PASSWORD, MNG_SOURCE, MNG_SSL, OUTPUT_COLNM, \
    GNDR_MALE, GNDR_FEMALE, MODEL2_COLNM, MODEL3_COLNM
from pymongo import MongoClient


def inc_gndr(counter_col, _id, gndr):
    """Increments the

    :param counter_col: counter collection
    :param _id: the name of the field to be incremented
    :type _id: str
    :return: the next subsequent number
    :rtype: int
    """
    inc = dict()
    if gndr == GNDR_MALE:
        oth_gndr=GNDR_FEMALE
    else:
        oth_gndr=GNDR_MALE

    inc['cnt.{}'.format(oth_gndr)] = 0
    inc['cnt.{}'.format(gndr)] = 1

    q = {'_id': _id}
    f = {'$inc': inc}
    # p = {'cnt.male': True, 'cnt.female': True, '_id': False}
    res = counter_col.find_one_and_update(q, f, upsert=True)

    if not res:
        return {gndr: 1, oth_gndr: 0}
    else:
        res['cnt'][gndr] += 1
        return res['cnt']


def mongo_connect(col_nm=None, dbname=MNG_DBNAME, host=MNG_HOST, port=MNG_PORT, user=MNG_USER_NAME, passwd=MNG_PASSWORD,
                  ssl=MNG_SSL, source=MNG_SOURCE):
    connection = MongoClient(host=host, port=port, ssl=ssl)
    if user:
        connection[dbname].authenticate(name=user, password=passwd, source=source)

    con = connection[dbname]
    if col_nm:
        con = con[col_nm]

    return con


def tbl_def():
    # con = mongo_connect()
    # con.create_collection(MODEL3_COLNM)
    con = mongo_connect(col_nm=MODEL3_COLNM)
    con.create_index('createdAt', expireAfterSeconds=1*60.0)

# con = (mongo_connect(colnm=MODEL2_COLNM))
# print(con)
# print(inc_gndr(con, 'test1', GNDR_MALE))
# con.insert({'_id': 'test'})
tbl_def()
