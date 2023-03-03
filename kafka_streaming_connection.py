import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, array, arrays_zip, explode, from_json
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, ArrayType, FloatType
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
import pandas as pd


#22 VARIABLES EXTRACTED FROM THE OPC SERVER DICT
dict_index = {'LOAD_1' : 0,
              'LOAD_2' : 1,
              'LOAD_3' : 2,
              'LOAD_4' : 3,
              'LOAD_5' : 4,
              'LOAD_6' : 5,
              'LOAD_SPINDLE' : 6,
              'POWER_1' : 7,
              'POWER_2' : 8,
              'POWER_3' : 9,
              'POWER_4' : 10,
              'POWER_5' : 11,
              'POWER_6' : 12,
              'RAPID_OVERRIDE' : 13,
              'RPM_1' : 14,
              'RPM_2' : 15,
              'RPM_3' : 16,
              'RPM_4' : 17,
              'RPM_5' : 18,
              'RPM_6' : 19,
              'RPM_SPINDLE' : 20,
              'SPINDLE_OVERRIDE' : 21
              }



#################################### DATASET (INICIO) ######################################


print("***************************************")
print("***************************************")
print("*****   CALCULANDO OUTLIERS ...   *****")
print("***************************************")
print("***************************************")

# Leer el documento CSV con los datos historicos
dfp = pd.read_csv("/home/ubuntu/totalDataNew.csv")

# Sacar información sobre el DataFrame
dfp.info()

# Borrar la columna 0 (la del timestamp)
dfp.drop(labels=dfp.columns[0], axis=1, inplace=True)

# Imprimir las primeras filas del DataFrame despues de haber borrado la columna del timestamp
print(dfp.head())

# Crear los arrays para agregar los límites (inferior y superior)
upper_bounds = []
lower_bounds = []

# Iterar sobre las columnas del DataFrame y sacar las medidas estadisticas (IQR y limites)
for column in dfp:
    Selected_column = dfp[column]
    print("Nombre de la columna: " + str(Selected_column.name))
    q1 = Selected_column.quantile(0.25)  # first quartile (Q1)
    print("QUARTIL 1= " + str(q1))
    q3 = Selected_column.quantile(0.75)  # third quartile (Q3)
    print("QUARTIL 3= " + str(q3))
    iqr = q3 - q1  # interquartile range (IQR)
    print("INTERQUARTILE RANGE= " + str(iqr))

    upper_limit = q3 + 1.5 * iqr # límite superior
    # guardar el nombre y su límite superior como tupla y agregarlos al array
    maxtuple = (str(Selected_column.name),upper_limit)
    upper_bounds.append(maxtuple)
    print("UPPER LIMIT= " + str(upper_limit))

    lower_limit = q1 - 1.5 * iqr # límite inferior
    # guardar el nombre y su límite inferior como tupla y agregarlos al array
    mintuple = (str(Selected_column.name), lower_limit)
    lower_bounds.append(mintuple)
    print("LOWER LIMIT= " + str(lower_limit))
    print("----------------------")

#################################### DATOS HISTORICOS (FIN) ###################################






#################################### TIEMPO REAL (INICIO) ########################################
spark = SparkSession \
    .builder \
    .appName("APP") \
    .getOrCreate()


# EJEMPLO DE FORMATO DE DATOS
# {"values":[{"id":"/Plc/IB","vd":"224","ts":"163546780"},
# {"id":"/Plc/IW","vd":"57344","ts":"163546781"},
# {"id":"/Plc/ID","vd":"3758101761","ts":"163546782"}]}

# Definir el schema para el topic Kafka_Topic
json_schema = ArrayType(StructType([StructField('id', StringType(), nullable=False), StructField('vd', FloatType(), nullable=False), StructField('ts', StringType(), nullable=False),]))

# Subscribirse al topic Kafka_Topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "IP_ADDRESS:PORT") \
    .option("subscribe", "Kafka_Topic") \
    .option("startingOffsets", "latest") \
    .load() \


df.printSchema()


# Parsear el JSON con los campos obtenidos del topic Kafka_Topic
def parse_json(array_str):
    json_obj = json.loads(array_str)
    for item in json_obj["values"]:
        yield (item["id"], item["vd"], item["ts"])


# Parsear el JSON con el schema
udf_parse_json = udf(lambda str: parse_json(str), json_schema)

# Crear un nuevo DataFrame para el array que contiene los valores del topic Kafka_Topic
df_new = df.select(udf_parse_json(df.value).alias("values"))

# Esquema con los datos del dataframe nuevo despues de parsear JSON
# (id, vd, ts)
df_new.printSchema()



# Seleccionar las columnas id, vd y ts
df_array_atributos = df_new.select(df_new.values.id.alias('id'), df_new.values.vd.alias('vd'), df_new.values.ts.alias('ts'))
df_array_atributos.printSchema()


# Convertir el array a filas del DataFrame con los métodos arrays_zip y explode
df_streaming = (df_array_atributos
          .withColumn("tmp", arrays_zip("id", "vd", "ts"))
          .withColumn("tmp", explode("tmp"))
          .select(col("tmp.id"), col("tmp.vd"), col("tmp.ts")))


df_streaming.printSchema()



# Este codigo funciona con el import influxdb_client
# Clase que gestiona lo que se debe hacer en cada batch (lote)
# En este caso se meten los records al measurement "measurement" de la BBDD "bucket"
class InfluxDBWriter:
    def __init__(self):
        self.client = InfluxDBClient(url="http://IP_ADDRESS:PORT", token="my-token", org="my-org")
        self.write_api = self.client.write_api()

    def open(self, partition_id, epoch_id):
        print("Opened %d, %d" % (partition_id, epoch_id))
        return True

    def process(self, row):
        self.write_api.write(bucket="bucket", record=self._row_to_line_protocol(row))

    def close(self, error):
        self.write_api.__del__()
        self.client.__del__()
        print("Closed with error: %s" % str(error))

    def _row_to_line_protocol(self, row: pyspark.sql.Row):

        timestampInt = int(row['ts'])

        # Restarle 400 segundos (400000 milisegundos) para obtener el timestamp en el que se hizo la petición al servidor OPC.
        timestampInt = timestampInt - 100000
        timestampString = str(timestampInt) + "000000"

        id = row['id']
        if id in dict_index:
            index = dict_index[id]
            # sacar el porcentaje respecto al limite superior de cada variable
            maxUpperBound = upper_bounds[index][1]
            if (maxUpperBound != 0):
                diferencia = ((row['vd'] / maxUpperBound) * 100)
            else:
                diferencia = row['vd'] * 100
            dif_absolute = abs(diferencia)
            porcentaje_red = round(dif_absolute)
            if (porcentaje_red > 1000):
                porcentaje_red = 1000
            if ((row['vd'] > maxUpperBound) or (row['vd'] < minUpperBound)):
                #print("SI es outlier")
                outlier = 1
            else:
                #print("NO es outlier")
                outlier = 0
            p = Point("measurement").tag("id", id).field("value", row['vd']).field("pct",porcentaje_red).time(int(timestampString))
            return p

# Clase InfluxDBWriter: https://github.com/influxdata/influxdb-client-python/issues/137
# Se utiliza forEach para tratar cada lote
query = df_streaming.writeStream.foreach(InfluxDBWriter()).start()
query.awaitTermination()

#################################### TIEMPO REAL (FIN) ###########################################
