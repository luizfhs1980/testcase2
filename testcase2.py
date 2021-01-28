#Pyspark Imports
from pyspark.sql import SparkSession 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("Test Case").getOrCreate()
		
def main():	

	#carrega um data frame com transações por municípios e data de atualização
	testcase2_data = (("1111110", "SAO PAULO", "SP", "15/01/2020 01:10:01"), \
    ("1111111", "SAO PAULO", "SP", "15/01/2020 01:15:01"), \
    ("1111112", "SAO PAULO", "SP", "20/01/2020 05:00:45"), \
    ("1111113", "SAO PAULO", "SP", "21/01/2020 06:00:05"), \
    ("1111114", "RIO DE JANEIRO", "RJ", "21/01/2020 07:00:45"), \
    ("1111115", "SAO PAULO", "SP", "23/01/2020 03:00:05"), \
    ("1111116", "RIO DE JANEIRO", "RJ", "23/01/2020 08:00:05"), \
	("1111117", "RIO DE JANEIRO", "RJ", "23/01/2020 08:01:05"), \
    ("1111118", "SAO PAULO", "SP", "25/01/2020 09:00:05"))
  
	columns= ["TRANSACAO", "MUNICIPIO", "ESTADO", "DATA_TRANSACAO"]
	
	df = spark.createDataFrame(data = testcase2_data, schema = columns)
	
	window  = Window.partitionBy("MUNICIPIO").orderBy("DATA_TRANSACAO")

	#data frame final com o campo e ordem solicitada
	df_Final = df.withColumn("ORDEM_TRANSACAO",row_number().over(window))
	
	df_Final.show()
	
if __name__ == "__main__":
	try:
		main()
	except:
		print("Erro no processamento.")