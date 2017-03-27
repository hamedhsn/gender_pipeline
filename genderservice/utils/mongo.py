from pymongo import MongoClient

from genderservice import configuration as cfg


def mongo_connect(col_nm=None, dbname=cfg.MNG_DBNAME, host=cfg.MNG_HOST, port=cfg.MNG_PORT,
                  user=cfg.MNG_USER_NAME, passwd=cfg.MNG_PASSWORD,
                  ssl=cfg.MNG_SSL, source=cfg.MNG_SOURCE):

    if ssl:
        connection = MongoClient(host=host, port=port, ssl=ssl)
    else:
        connection = MongoClient(host=host, port=port)

    if user:
        connection[dbname].authenticate(name=user, password=passwd, source=source)

    con = connection[dbname]
    if col_nm:
        con = con[col_nm]

    return con


def tbl_def(num_days=7):
    """ create a collection with expiration time for records

    :param num_days: number of days
    """
    day = 3600*24.0
    exp_time = num_days*day

    con = mongo_connect(col_nm=cfg.MODEL3_COLNM)

    con.create_index('createdAt', expireAfterSeconds=exp_time)

