# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9178ac48-d3cb-4e4a-9780-1b5942be7480",
# META       "default_lakehouse_name": "LK_Healt_Data_Project",
# META       "default_lakehouse_workspace_id": "74f8d0e1-c7ee-413b-9861-337a4736d6dc",
# META       "known_lakehouses": [
# META         {
# META           "id": "9178ac48-d3cb-4e4a-9780-1b5942be7480"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Carga Bronze — Raw (CSV) para tabelas Delta

#- Todas as colunas entram como texto (sem conversão de tipo); a tipagem fica para a camada Silver.
#- Cada tabela ganha duas colunas de auditoria: `_arquivo_origem` e `_data_carga`.
#- O arquivo duplicado `STG_EHP__TMMD.csv` fica de fora.

#**Antes de executar:** anexe o Lakehouse `LK_Healt_Data_Project` ao notebook como Lakehouse padrão.
# Parâmetros
RAW = "Files/Raw"      # pasta dos CSV (o nome diferencia maiúsculas de minúsculas)
SCHEMA = "Bronze"      # schema de destino no Lakehouse

# tabela -> (caminho dos arquivos dentro de RAW, linhas esperadas segundo o dicionário de dados)
TABELAS = {
    "patn": ("STG_EHP__PATN.csv", 351765),
    "algy": ("STG_EHP__ALGY.csv", 44),
    "ptal": ("STG_EHP__PTAL.csv", 644303),
    "insr": ("STG_EHP__INSR.csv", 457072),
    "appt": ("STG_EHP__APPT.csv", 974032),
    "vist": ("STG_EHP__VIST.csv", 917331),
    "trtm": ("STG_EHP__TRTM.csv", 917191),
    "bill": ("STG_EHP__BILL.csv", 917331),
    "mdbl": ("STG_EHP__MDBL.csv", 465152),
    "dpmt": ("STG_EHP__DPMT.csv", 30),
    "roms": ("STG_EHP__ROMS.csv", 24302),
    "stff": ("STG_EHP__STFF.csv", 5496),
    "medt": ("STG_EHP__MEDT.csv", 312870),
    "mdcn": ("STG_EHP__MDCN.csv", 346302),
    "eqpm": ("STG_EHP__EQPM.csv", 10190),
    "vndr": ("STG_EHP__VNDR.csv", 13562),
    "sply": ("STG_EHP__SPLY.csv", 721134),
    "diag": ("STG_EHP__DIAG/*.csv", 1602174),
    "pmnt": ("STG_EHP__PMNT/*.csv", 2774514),
    "vpmt": ("STG_EHP__VPMT/*.csv", 1802488),
    # só os arquivos _1 a _4: STG_EHP__TMMD.csv é uma cópia duplicada
    "tmmd": ("STG_EHP__TMMD/STG_EHP__TMMD_*.csv", 3209411),
    # 98 arquivos, 6,4 GB: é a tabela mais demorada
    "invn": ("STG_EHP__INVN/*.csv", 90731353),
}
from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

def carregar(tabela, caminho):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)   # tudo como texto
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"{RAW}/{caminho}")
        .withColumn("_arquivo_origem", F.col("_metadata.file_name"))
        .withColumn("_data_carga", F.current_timestamp())
    )
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(f"{SCHEMA}.{tabela}")
    )
# Carga de todas as tabelas
resultado = []
for tabela, (caminho, esperado) in TABELAS.items():
    try:
        carregar(tabela, caminho)
        linhas = spark.table(f"{SCHEMA}.{tabela}").count()
        status = "OK" if linhas == esperado else "DIFERENTE"
    except Exception as e:
        linhas, status = None, f"ERRO: {str(e)[:200]}"
    resultado.append((tabela, esperado, linhas, status))
    print(tabela, esperado, linhas, status)
## Conferência

#Compara as linhas carregadas com as contagens do dicionário de dados. Todas as tabelas devem aparecer com status `OK`.
conf = spark.createDataFrame(resultado, "tabela string, esperado long, carregado long, status string")
display(conf.orderBy("tabela"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Interrompe o pipeline se alguma tabela não carregou corretamente
problemas = [r for r in resultado if not str(r[3]).startswith("OK")]
if problemas:
    raise Exception(f"{len(problemas)} tabela(s) com problema: {problemas}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
