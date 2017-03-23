
# KAFKA_CLUSTR = ['127.0.0.1:9092']
KAFKA_CLUSTR = ['52.209.237.30:9092', '52.209.214.8:9092', '52.208.211.213:9092']


SERIALIZATIO_AVRO = 'avro'
SERIALIZATIO_JSON = 'json'

KFK_PRODUCER = 'Producer'
KFK_CONSUMER = 'Consumer'

# ##### AVRO Schema if Any #####
avro_test_schema = {
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number", "type": ["int", "null"]},
        {"name": "favorite_color", "type": ["string", "null"]}
    ]
}

INGESTION_TOPIC = 'gndr-ingst'
GNDR_SERV_TOPIC = 'gndr-rslt'


CONSUMER_GRP_MODEL1 = 'CS-MODEL1'
CONSUMER_GRP_MODEL2 = 'CS-MODEL2'
CONSUMER_GRP_MODEL3 = 'CS-MODEL3'

# MNG_HOST = "analytics-shard-00-00-tyhwz.mongodb.net",
# MNG_HOST = "analytics-shard-00-01-tyhwz.mongodb.net"
MNG_HOST = "analytics-shard-00-02-tyhwz.mongodb.net"
MNG_PORT = 27017
MNG_USER_NAME = "dm_eng_rw"
MNG_DBNAME = "analytics"
MNG_PASSWORD = "EYxdZw4Uk7G85SM"
MNG_SSL = True
MNG_SOURCE = "admin"

MODEL1_COLNM = 'Model1'
MODEL2_COLNM = 'Model2'
MODEL3_COLNM = 'Model3'

OUTPUT_COLNM = 'gender'

GNDR_MALE = 'male'
GNDR_FEMALE = 'female'
