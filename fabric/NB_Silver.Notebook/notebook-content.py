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

# MARKDOWN ********************

# # Camada Silver — limpeza e tipagem
# 
# Lê as tabelas do schema `Bronze` e grava as tabelas tratadas no schema `Silver` do Lakehouse `LK_Healt_Data_Project`.
# 
# O que o notebook faz, com base na aba **Qualidade** do dicionário de dados:
# 
# 1. **Textos:** remove espaços e converte vazio e o texto `'NULL'` em nulo.
# 2. **Tipos:** converte códigos para inteiro, valores para decimal com 2 casas e datas (nos formatos `AAAA-MM-DD` e `M/D/AAAA`) para `date` ou `timestamp`.
# 3. **Chaves:** completa `ALG_ID` com zeros à esquerda na `ptal` e remove duplicados em `stff`, `roms`, `medt`, `ptal` e `invn`.
# 4. **Marcações:** indica a origem da visita (agendamento, E ou W), os `REFR_NO` repetidos e o tipo de material (medicamento ou equipamento) nos fornecimentos.
# 5. **Conferência:** mostra as linhas por tabela e quantos valores não puderam ser convertidos.
# 
# **Antes de executar:** anexe o Lakehouse `LK_Healt_Data_Project` como Lakehouse padrão e importe este arquivo como notebook (não cole o conteúdo numa célula só).


# MARKDOWN ********************

# ## 1. Parâmetros e configuração

# CELL ********************

from pyspark.sql import functions as F, Window as W

BRONZE = "Bronze"
SILVER = "Silver"

# Datas inválidas viram nulo em vez de interromper a execução
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")

I, D, DT, TS = "int", "decimal", "date", "timestamp"

# Tipo de cada coluna que não é texto. As demais continuam como texto.
# "esperado" = linhas que a tabela deve ter na Silver (dicionário de dados, já sem duplicados).
SPEC = {
    "algy": dict(tipos={}, esperado=44),
    "dpmt": dict(tipos={}, esperado=30),
    "patn": dict(tipos={"GEN_CD": I, "DT_BRT": DT, "NAT_ID_E": DT, "INGST_TMSTMP": TS}, esperado=351765),
    "stff": dict(tipos={"GEN_CD": I, "DT_BRT": DT, "ROLE_CD": I, "STAT_CD": I, "HIRE_DATE": DT, "ROLE_DATE": DT,
                        "SAL_YR": D, "SAL_MN": D, "INGST_TMSTMP": TS}, esperado=5411),
    "roms": dict(tipos={"PRPS_CD": I, "RTYPE_CD": I, "CPCT_NO": I}, esperado=24298),
    "medt": dict(tipos={"TEAM_NO": I, "ROLE_CD": I}, esperado=312635),
    "mdcn": dict(tipos={"TYPE_CD": I, "FORM_CD": I, "STRN_NO": D, "STRG_CD": I, "FMQTY_NO": D, "MSTAT_CD": I,
                        "EXP_MON": I, "UN_P_CS": I, "COST_UN": D}, esperado=346302),
    "eqpm": dict(tipos={"TYPE_CD": I, "ESTAT_CD": I}, esperado=10190),
    "vndr": dict(tipos={"VSTAT_CD": I, "INGST_TMSTMP": TS}, esperado=13562),
    "insr": dict(tipos={"ISTAT_CD": I, "POL_ST": DT, "POL_ED": DT}, esperado=457072),
    "ptal": dict(tipos={"SVR_CD": I, "RCT_CD": I}, esperado=631054),
    "appt": dict(tipos={"APT_TIME": TS, "ASTAT_CD": I, "ATYPE_CD": I}, esperado=974032),
    "vist": dict(tipos={"VIS_EN": TS, "VIS_EX": TS, "VSTAT_CD": I, "VTYPE_CD": I}, esperado=917331),
    "diag": dict(tipos={"DIG_TOT": I, "DIG_EN": TS, "DIG_SEQ": I, "DIG_CD": I, "DSTAT_CD": I, "DTYPE_CD": I}, esperado=1602174),
    "trtm": dict(tipos={"TRTM_TOT": I, "TRTM_EN": TS, "TTYPE_CD": I, "TRTM_CD": I, "TSTAT_CD": I, "TRTM_SEQ": I}, esperado=917191),
    "tmmd": dict(tipos={"MEDS_TOT": I, "MEDS_SEQ": I, "QTY_NO": D, "DOS_CD": I, "FREQ_CD": I, "RUTE_CD": I, "UNIT_DOS": D}, esperado=3209411),
    "bill": dict(tipos={"BILL_DATE": TS, "BILL_AMT": D, "BSTAT_CD": I}, esperado=917331),
    "mdbl": dict(tipos={"BILL_DATE": TS, "BILL_AMT": D, "BSTAT_CD": I}, esperado=465152),
    "pmnt": dict(tipos={"TOT_PMNT": D, "PMNT_TMS": I, "PMNT_NO": I, "PMNT_DATE": TS, "PMNT_AMT": D, "METH_CD": I, "PSTAT_CD": I}, esperado=2774514),
    "sply": dict(tipos={"BTCH_SZ": I, "MFG_DATE": DT, "SPL_QTY": D, "SPL_UCT": D, "RCVD_DATE": DT, "RCVD_QTY": D, "VRFC_DATE": DT}, esperado=721134),
    "vpmt": dict(tipos={"SPL_CST": D, "PMNT_TRM": I, "TRM_NO": I, "VPMT_AMT": D, "PMNT_DATE": DT, "PMNT_DUE": D, "VPSTATUS_CD": I}, esperado=1802488),
    "invn": dict(tipos={"INGTN_TS": DT, "OPEN_QTY": D, "RCVD_QTY": D, "ISSD_QTY": D, "CLSD_QTY": D}, esperado=90731124),
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Funções genéricas

# CELL ********************

AUDITORIA = ["_arquivo_origem", "_data_carga"]

def limpar_textos(df):
    """Remove espaços e converte vazio e o texto 'NULL' em nulo em todas as colunas de texto."""
    for c, t in df.dtypes:
        if t == "string" and c not in AUDITORIA:
            v = F.trim(F.col(c))
            df = df.withColumn(c, F.when((v == "") | (F.upper(v) == "NULL"), None).otherwise(v))
    return df

def converter(c, tipo):
    """Expressão de conversão de uma coluna de texto para o tipo indicado."""
    col = F.col(c)
    if tipo == I:
        return col.cast("int")
    if tipo == D:
        return F.round(col.cast("double"), 2).cast("decimal(18,2)")
    if tipo == DT:
        return F.coalesce(F.to_date(col, "yyyy-MM-dd"), F.to_date(col, "M/d/yyyy"),
                          F.to_date(F.to_timestamp(col, "M/d/yyyy H:mm")))
    if tipo == TS:
        return F.coalesce(F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss"), F.to_timestamp(col, "M/d/yyyy H:mm"),
                          F.to_timestamp(col, "yyyy-MM-dd"), F.to_timestamp(col, "M/d/yyyy"))
    raise ValueError(tipo)

def tipar(df, tipos):
    """Converte as colunas e conta quantos valores preenchidos não puderam ser convertidos."""
    falhas = []
    for c, t in tipos.items():
        df = df.withColumn(f"__{c}", converter(c, t))
        falhas.append(F.sum(F.when(F.col(c).isNotNull() & F.col(f"__{c}").isNull(), 1).otherwise(0)).alias(c))
    contagem = df.agg(*falhas).first().asDict() if falhas else {}
    for c in tipos:
        df = df.drop(c).withColumnRenamed(f"__{c}", c)
    return df, {k: v for k, v in contagem.items() if v}

def manter_um(df, chave, ordem):
    """Mantém uma linha por chave, escolhida pela ordem indicada."""
    w = W.partitionBy(*chave).orderBy(*ordem)
    return df.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")

def gravar(df, tabela, colunas_originais):
    """Mantém a ordem original das colunas, acrescenta a auditoria da Silver e grava em Delta."""
    extras = [c for c in df.columns if c not in colunas_originais and c not in AUDITORIA]
    base = [c for c in colunas_originais if c in df.columns and c not in AUDITORIA]
    df = (df.select(*base, *extras, *AUDITORIA)
            .withColumnRenamed("_data_carga", "_data_carga_bronze")
            .withColumn("_data_carga_silver", F.current_timestamp()))
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
       .saveAsTable(f"{SILVER}.{tabela}"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Regras específicas por tabela
# 
# Cada regra recebe a tabela já limpa e tipada e devolve a versão corrigida. As tabelas sem regra passam só pela limpeza e tipagem.
# 
# | Tabela | Regra |
# |---|---|
# | `ptal` | `ALG_ID` com 3 dígitos (`7` → `007`); um registro por paciente + alergia, ficando o de maior gravidade |
# | `stff` | um registro por `STF_ID`, o de ingestão mais recente |
# | `roms`, `medt` | um registro por chave |
# | `eqpm` | `EQP_CAT__1/2/3` renomeadas para `EQP_CAT_1/2/3`, como no ERD |
# | `vist` | `REFR_ORIGEM` (AGENDAMENTO, E ou W) e `REFR_DUPLICADO` |
# | `bill`, `mdbl` | `REFR_DUPLICADO` |
# | `diag`, `trtm`, `tmmd` | `REFR_AMBIGUO`: a visita tem `REFR_NO` repetido e não dá para saber o paciente |
# | `sply` | `MAT_TIPO` (MEDICAMENTO ou EQUIPAMENTO) |
# | `invn` | um registro por medicamento + dia |

# CELL ********************

def refr_duplicado(df):
    return df.withColumn("REFR_DUPLICADO", F.count("*").over(W.partitionBy("REFR_NO")) > 1)

def refr_ambiguo(df):
    dup = (spark.table(f"{SILVER}.vist").filter("REFR_DUPLICADO")
             .select("REFR_NO").distinct().withColumn("REFR_AMBIGUO", F.lit(True)))
    return df.join(dup, "REFR_NO", "left").fillna({"REFR_AMBIGUO": False})

def r_ptal(df):
    df = df.withColumn("ALG_ID", F.lpad("ALG_ID", 3, "0"))
    return manter_um(df, ["PAT_ID", "ALG_ID"], [F.col("SVR_CD").desc_nulls_last(), F.col("RCT_CD")])

def r_stff(df):
    return manter_um(df, ["STF_ID"], [F.col("INGST_TMSTMP").desc_nulls_last()])

def r_roms(df):
    return manter_um(df, ["ROM_ID"], [F.col("DEP_ID")])

def r_medt(df):
    return manter_um(df, ["MEDT_ID", "STF_ID"], [F.col("ROLE_CD")])

def r_eqpm(df):
    for n in (1, 2, 3):
        df = df.withColumnRenamed(f"EQP_CAT__{n}", f"EQP_CAT_{n}")
    return df

def r_vist(df):
    df = df.withColumn("REFR_ORIGEM",
                       F.when(F.col("REFR_NO").rlike("^[0-9]+$"), "AGENDAMENTO")
                        .otherwise(F.substring("REFR_NO", 1, 1)))
    return refr_duplicado(df)

def r_sply(df):
    med = spark.table(f"{SILVER}.mdcn").select(F.col("MED_ID").alias("MAT_SKU")).withColumn("__med", F.lit(1))
    eqp = spark.table(f"{SILVER}.eqpm").select(F.col("EQP_ID").alias("MAT_SKU")).withColumn("__eqp", F.lit(1))
    df = df.join(med, "MAT_SKU", "left").join(eqp, "MAT_SKU", "left")
    df = df.withColumn("MAT_TIPO", F.when(F.col("__med") == 1, "MEDICAMENTO")
                                    .when(F.col("__eqp") == 1, "EQUIPAMENTO"))
    return df.drop("__med", "__eqp")

def r_invn(df):
    return df.dropDuplicates(["MAT_SKU", "INGTN_TS"])

REGRAS = {
    "ptal": r_ptal, "stff": r_stff, "roms": r_roms, "medt": r_medt, "eqpm": r_eqpm,
    "vist": r_vist, "bill": refr_duplicado, "mdbl": refr_duplicado,
    "diag": refr_ambiguo, "trtm": refr_ambiguo, "tmmd": refr_ambiguo,
    "sply": r_sply, "invn": r_invn,
}
# Mapa de nomes renomeados (para manter a ordem original das colunas)
RENOMEADAS = {f"EQP_CAT__{n}": f"EQP_CAT_{n}" for n in (1, 2, 3)}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Execução
# 
# As tabelas são processadas na ordem do `SPEC`: dimensões primeiro, porque `vist`, `mdcn` e `eqpm` são usadas nas regras de outras tabelas. A `invn` (90,7 milhões de linhas) é a mais demorada.

# CELL ********************

resultado = []
for tabela, cfg in SPEC.items():
    try:
        bronze = spark.table(f"{BRONZE}.{tabela}")
        originais = [RENOMEADAS.get(c, c) for c in bronze.columns]
        df = limpar_textos(bronze)
        df, falhas = tipar(df, cfg["tipos"])
        if tabela in REGRAS:
            df = REGRAS[tabela](df)
        gravar(df, tabela, originais)
        linhas = spark.table(f"{SILVER}.{tabela}").count()
        status = "OK" if linhas == cfg["esperado"] else "DIFERENTE"
        if falhas:
            status += " (com falhas de conversão)"
    except Exception as e:
        linhas, falhas, status = None, {}, f"ERRO: {str(e)[:300]}"
    resultado.append((tabela, cfg["esperado"], linhas, status, str(falhas) if falhas else ""))
    print(tabela, linhas, status, falhas or "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Conferência
# 
# - **status `OK`:** a tabela tem o número de linhas esperado.
# - **falhas de conversão:** colunas em que algum valor preenchido não pôde ser convertido para o tipo definido (e virou nulo), com a quantidade de valores. O esperado é que essa coluna fique vazia em todas as tabelas.

# CELL ********************

conf = spark.createDataFrame(
    resultado, "tabela string, esperado long, carregado long, status string, falhas_conversao string")
display(conf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Verificações pontuais (opcional)

# CELL ********************

# Origem das visitas: deve dar 250.245 AGENDAMENTO, 326.043 E e 341.043 W
display(spark.table(f"{SILVER}.vist").groupBy("REFR_ORIGEM").count())

# Tipo de material nos fornecimentos: nenhuma linha deve ficar com MAT_TIPO nulo
display(spark.table(f"{SILVER}.sply").groupBy("MAT_TIPO").count())

# Alergias: todos os ALG_ID da ptal devem existir na algy (resultado esperado: 0)
print(spark.table(f"{SILVER}.ptal").join(spark.table(f"{SILVER}.algy"), "ALG_ID", "left_anti").count())

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
