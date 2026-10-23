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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Camada Gold — modelo em estrela
# 
# Lê as tabelas do schema `Silver` e grava no schema `Gold` as dimensões e fatos que o Power BI vai usar.
# 
# | Dimensões | Fatos |
# |---|---|
# | `dim_paciente`, `dim_funcionario`, `dim_departamento`, `dim_sala`, `dim_medicamento`, `dim_material`, `dim_fornecedor`, `dim_calendario` | `fato_agendamento`, `fato_visita`, `fato_diagnostico`, `fato_tratamento`, `fato_prescricao`, `fato_faturamento`, `fato_pagamento`, `fato_fornecimento`, `fato_estoque_mensal` |
# 
# **Regras de modelagem:**
# 
# - **Chaves substitutas (`sk_`):** número gerado a partir da chave de negócio (hash), sempre o mesmo para o mesmo valor. Assim fatos e dimensões se ligam sem tabela de controle e a carga pode ser refeita.
# - **Membro "Não informado":** toda dimensão tem uma linha com `sk = -1`; os fatos sem a referência apontam para ela, e o Power BI não perde linhas.
# - **Datas:** cada fato tem chaves `data_..._key` no formato `AAAAMMDD`, ligadas à `dim_calendario`.
# - **Visitas ambíguas:** diagnósticos, tratamentos e prescrições de visitas com `REFR_NO` ambíguo ficam de fora desses três fatos (não dá para saber o paciente). Elas continuam em `fato_visita`.
# - **Dados pessoais:** e-mail, telefone e documento do paciente não vão para a Gold.
# 
# **Antes de executar:** anexe o Lakehouse `LK_Healt_Data_Project` como Lakehouse padrão e importe este arquivo como notebook. A Silver precisa ter rodado antes.


# MARKDOWN ********************

# ## 1. Parâmetros e funções

# CELL ********************

from pyspark.sql import functions as F, Window as W

SILVER = "Silver"
GOLD = "Gold"
NI = "Unknown"
ANO_INICIO, ANO_FIM = 2023, 2026   # intervalo da dim_calendario (cobre todas as datas dos fatos)
DATA_REFERENCIA = "2025-09-30"     # data usada para calcular a idade do paciente (fim do período dos dados)


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")
s = lambda t: spark.table(f"{SILVER}.{t}")
NI = "Não informado"

def sk(*cols):
    """Chave substituta: hash da chave de negócio; -1 quando alguma parte for nula."""
    nulo = F.lit(False)
    for c in cols:
        nulo = nulo | F.col(c).isNull()
    return F.when(nulo, F.lit(-1).cast("long")).otherwise(F.xxhash64(*[F.col(c) for c in cols]))

def data_key(col):
    """Chave de data AAAAMMDD; -1 quando a data for nula."""
    return F.coalesce(F.date_format(F.col(col), "yyyyMMdd").cast("int"), F.lit(-1))

def com_desconhecido(df, chave):
    """Acrescenta a linha 'Não informado' (sk = -1) à dimensão."""
    linha = {}
    for c, t in df.dtypes:
        if c == chave:
            linha[c] = F.lit(-1).cast(t)
        elif t == "string":
            linha[c] = F.lit(NI)
        else:
            linha[c] = F.lit(None).cast(t)
    return df.unionByName(spark.range(1).select(*[linha[c].alias(c) for c in df.columns]))

def gravar(df, tabela):
    (df.withColumn("_data_carga_gold", F.current_timestamp())
       .write.format("delta").mode("overwrite").option("overwriteSchema", True)
       .saveAsTable(f"{GOLD}.{tabela}"))
    n = spark.table(f"{GOLD}.{tabela}").count()
    print(f"{tabela}: {n:,} linhas".replace(",", "."))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Dimensões

# CELL ********************

# dim_calendario
meses = F.array(*[F.lit(m) for m in ["January", "February", "March", "April", "May", "June", "July",
                                     "August", "September", "October", "November", "December"]])
dias = F.array(*[F.lit(d) for d in ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]])
abrev = F.array(*[F.lit(m) for m in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]])
cal = (spark.sql(f"SELECT explode(sequence(to_date('{ANO_INICIO}-01-01'), to_date('{ANO_FIM}-12-31'), interval 1 day)) AS data")
       .select(
           data_key("data").alias("data_key"),
           "data",
           F.year("data").alias("ano"),
           F.quarter("data").alias("trimestre"),
           F.month("data").alias("mes"),
           meses[F.month("data") - 1].alias("nome_mes"),
           F.date_format("data", "yyyy-MM").alias("ano_mes"),
           F.concat(abrev[F.month("data") - 1], F.lit("/"), F.date_format("data", "yy")).alias("mes_ano"),   # ex.: jan/24
           (F.year("data") * 100 + F.month("data")).alias("mes_ano_ordem"),                                 # ex.: 202401
           F.dayofmonth("data").alias("dia"),
           F.dayofweek("data").alias("dia_semana"),
           dias[F.dayofweek("data") - 1].alias("nome_dia_semana"),
           F.dayofweek("data").isin(1, 7).alias("fim_de_semana")))
gravar(com_desconhecido(cal, "data_key"), "dim_calendario")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_departamento
dep = s("dpmt").select(sk("DEP_ID").alias("sk_departamento"), "DEP_ID",
                       F.col("DEP_NAME").alias("departamento"), F.col("DEP_DES").alias("descricao"),
                       F.col("DEP_LOC").alias("localizacao"))
gravar(com_desconhecido(dep, "sk_departamento"), "dim_departamento")

# dim_sala
sala = (s("roms").join(s("dpmt").select("DEP_ID", "DEP_NAME"), "DEP_ID", "left")
        .select(sk("ROM_ID").alias("sk_sala"), "ROM_ID", sk("DEP_ID").alias("sk_departamento"),
                F.col("DEP_NAME").alias("departamento"), F.col("PRPS_DES").alias("finalidade"),
                F.col("RTYPE_DES").alias("porte"), F.col("CPCT_NO").alias("capacidade")))
gravar(com_desconhecido(sala, "sk_sala"), "dim_sala")

# dim_funcionario
func = (s("stff").join(s("dpmt").select("DEP_ID", "DEP_NAME"), "DEP_ID", "left")
        .select(sk("STF_ID").alias("sk_funcionario"), "STF_ID",
                F.concat_ws(" ", "F_NAME", "L_NAME").alias("nome"),
                F.col("GEN_DES").alias("genero"), F.col("ROLE_DES").alias("cargo"),
                sk("DEP_ID").alias("sk_departamento"), F.col("DEP_NAME").alias("departamento"),
                F.col("STAT_DES").alias("situacao"), F.col("HIRE_DATE").alias("data_contratacao"),
                F.col("SAL_YR").alias("salario_anual")))
gravar(com_desconhecido(func, "sk_funcionario"), "dim_funcionario")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_paciente (sem e-mail, telefone e documento)
seguro = s("insr").groupBy("PAT_ID").agg(
    F.count("*").alias("qtd_apolices"),
    F.max(F.when(F.col("ISTAT_CD") == 1, True).otherwise(False)).alias("possui_seguro_ativo"))
alergia = s("ptal").groupBy("PAT_ID").agg(F.count("*").alias("qtd_alergias"))
idade = F.floor(F.months_between(F.lit(DATA_REFERENCIA).cast("date"), F.col("DT_BRT")) / 12).cast("int")
pac = (s("patn").join(seguro, "PAT_ID", "left").join(alergia, "PAT_ID", "left")
       .withColumn("idade", idade)
       .select(sk("PAT_ID").alias("sk_paciente"), "PAT_ID",
               F.concat_ws(" ", "F_NAME", "L_NAME").alias("nome"),
               F.col("GEN_DES").alias("genero"), F.col("DT_BRT").alias("data_nascimento"), "idade",
               F.when(F.col("idade") < 18, "0-17").when(F.col("idade") < 30, "18-29")
                .when(F.col("idade") < 45, "30-44").when(F.col("idade") < 60, "45-59")
                .when(F.col("idade").isNotNull(), "60+").otherwise(NI).alias("faixa_etaria"),
               F.col("B_TYPE").alias("tipo_sanguineo"), F.col("MAR_ST").alias("estado_civil"),
               F.col("NAT_ID_S").alias("uf_documento"),
               F.coalesce("qtd_apolices", F.lit(0)).alias("qtd_apolices"),
               F.coalesce("possui_seguro_ativo", F.lit(False)).alias("possui_seguro_ativo"),
               F.coalesce("qtd_alergias", F.lit(0)).alias("qtd_alergias")))
gravar(com_desconhecido(pac, "sk_paciente"), "dim_paciente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_medicamento
med = s("mdcn").select(sk("MED_ID").alias("sk_medicamento"), "MED_ID",
                       F.col("MED_NAME").alias("medicamento"), F.col("TYPE_DES").alias("classe"),
                       F.col("FORM_DES").alias("forma"),
                       F.concat_ws(" ", F.col("STRN_NO").cast("string"), "STRN_UN").alias("concentracao"),
                       F.col("STRG_DES").alias("armazenamento"), F.col("MSTAT_DES").alias("status"),
                       F.col("COST_UN").alias("custo_embalagem"))
gravar(com_desconhecido(med, "sk_medicamento"), "dim_medicamento")

# dim_material (medicamentos + equipamentos; usada nos fornecimentos)
mat = (s("mdcn").select(F.col("MED_ID").alias("MAT_SKU"), F.lit("Medication").alias("tipo_material"),
                        F.col("MED_NAME").alias("material"), F.col("TYPE_DES").alias("classe"))
       .unionByName(s("eqpm").select(F.col("EQP_ID").alias("MAT_SKU"), F.lit("Equipment").alias("tipo_material"),
                                     F.col("EQP_NAME").alias("material"), F.col("TYPE_DES").alias("classe")))
       .select(sk("MAT_SKU").alias("sk_material"), "*"))
gravar(com_desconhecido(mat, "sk_material"), "dim_material")

# dim_fornecedor
forn = s("vndr").select(sk("VEN_ACC").alias("sk_fornecedor"), "VEN_ACC",
                        F.col("VEN_NAME").alias("fornecedor"), F.col("VEN_TYPE").alias("tipo"),
                        F.col("CTRY_NAME").alias("pais"), F.col("VSTAT_DES").alias("status"))
gravar(com_desconhecido(forn, "sk_fornecedor"), "dim_fornecedor")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Fatos clínicos

# CELL ********************

# fato_agendamento: 1 linha por agendamento
agd = s("appt").select(
    F.col("REFR_NO").alias("num_agendamento"),
    sk("PAT_ID").alias("sk_paciente"),
    sk("DOC_ID").alias("sk_medico"),
    sk("CTD_ID").alias("sk_atendente"),
    data_key("APT_TIME").alias("data_agendamento_key"),
    F.col("APT_TIME").alias("data_hora_agendamento"),
    F.col("ATYPE_DES").alias("tipo"),
    F.col("ASTAT_DES").alias("status"),
    F.when(F.col("ASTAT_DES") == "Completed", "Completed")
     .when(F.col("ASTAT_DES") == "No show", "No show")
     .when(F.col("ASTAT_DES") == "Cancelled", "Cancelled")
     .when(F.col("ASTAT_DES") == "In Future", "Futuro")
     .otherwise("Não informado").alias("status_descricao"),
    (F.col("ASTAT_DES") == "Completed").cast("int").alias("compareceu"),
    (F.col("ASTAT_DES") == "Cancelled").cast("int").alias("cancelado"),
    (F.col("ASTAT_DES") == "No show").cast("int").alias("faltou"))
gravar(agd, "fato_agendamento")

# fato_visita: 1 linha por visita (REFR_NO + paciente)
vis = s("vist").select(
    sk("REFR_NO", "PAT_ID").alias("sk_visita"),
    F.col("REFR_NO").alias("num_visita"),
    sk("PAT_ID").alias("sk_paciente"),
    sk("ROM_ID").alias("sk_sala"),
    F.col("MEDT_ID").alias("equipe_medica"),
    data_key("VIS_EN").alias("data_entrada_key"),
    data_key("VIS_EX").alias("data_saida_key"),
    F.col("VIS_EN").alias("data_hora_entrada"),
    F.col("VIS_EX").alias("data_hora_saida"),
    F.round((F.unix_timestamp("VIS_EX") - F.unix_timestamp("VIS_EN")) / 86400, 2).alias("permanencia_dias"),
    F.col("VTYPE_DES").alias("tipo"),
    F.col("VSTAT_DES").alias("status"),
    F.col("REFR_ORIGEM").alias("origem"),
    F.when(F.col("REFR_ORIGEM") == "AGENDAMENTO", "Scheduled")
     .when(F.col("REFR_ORIGEM") == "E", "Walk-in (E)")
     .when(F.col("REFR_ORIGEM") == "W", "Walk-in (W)")
     .otherwise("Não informado").alias("Unknown"),
    F.col("REFR_DUPLICADO").alias("num_visita_repetido"))
gravar(vis, "fato_visita")

# Visitas não ambíguas: REFR_NO -> paciente (base para diagnóstico, tratamento e prescrição)
visita_unica = s("vist").filter(~F.col("REFR_DUPLICADO")).select("REFR_NO", "PAT_ID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fato_diagnostico: 1 linha por diagnóstico da visita
diag = (s("diag").filter(~F.col("REFR_AMBIGUO")).join(visita_unica, "REFR_NO", "left")
        .select(sk("REFR_NO", "PAT_ID").alias("sk_visita"), F.col("REFR_NO").alias("num_visita"),
                sk("PAT_ID").alias("sk_paciente"), sk("ROM_ID").alias("sk_sala"),
                data_key("DIG_EN").alias("data_key"),
                F.col("DIG_SEQ").alias("sequencia"), F.col("DIG_CD").alias("cod_diagnostico"),
                F.col("DIG_DES").alias("diagnostico"), F.col("DSTAT_DES").alias("status"),
                F.col("DTYPE_DES").alias("conduta_recomendada")))
gravar(diag, "fato_diagnostico")

# fato_tratamento: 1 linha por tratamento da visita
trat = (s("trtm").filter(~F.col("REFR_AMBIGUO")).join(visita_unica, "REFR_NO", "left")
        .select(sk("REFR_NO", "PAT_ID").alias("sk_visita"), F.col("REFR_NO").alias("num_visita"),
                sk("PAT_ID").alias("sk_paciente"), data_key("TRTM_EN").alias("data_key"),
                F.col("TRTM_SEQ").alias("sequencia"), F.col("TTYPE_DES").alias("tipo"),
                F.col("TRTM_CD").alias("cod_tratamento"), F.col("TRTM_DES").alias("tratamento"),
                F.col("TSTAT_DES").alias("status")))
gravar(trat, "fato_tratamento")

# fato_prescricao: 1 linha por medicamento prescrito na visita
custo = s("mdcn").select("MED_ID", "COST_UN")
data_visita = s("vist").filter(~F.col("REFR_DUPLICADO")).select("REFR_NO", "VIS_EN")
presc = (s("tmmd").filter(~F.col("REFR_AMBIGUO")).join(visita_unica, "REFR_NO", "left")
         .join(data_visita, "REFR_NO", "left").join(custo, "MED_ID", "left")
         .select(sk("REFR_NO", "PAT_ID").alias("sk_visita"), F.col("REFR_NO").alias("num_visita"),
                 sk("PAT_ID").alias("sk_paciente"), sk("MED_ID").alias("sk_medicamento"),
                 data_key("VIS_EN").alias("data_key"),
                 F.col("MEDS_SEQ").alias("sequencia"), F.col("QTY_NO").alias("quantidade"),
                 F.col("DOS_DES").alias("posologia"), F.col("FREQ_DES").alias("duracao"),
                 F.col("RUTE_DES").alias("via"), F.col("UNIT_DOS").alias("unidades_por_dose"),
                 # estimativa: quantidade prescrita x custo da embalagem
                 (F.col("QTY_NO") * F.col("COST_UN")).cast("decimal(18,2)").alias("custo_estimado")))
gravar(presc, "fato_prescricao")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Fatos financeiros

# CELL ********************

# Total pago pelo paciente por fatura (somente parcelas concluídas)
pago = (s("pmnt").filter(F.col("PSTAT_CD") == 0)
        .groupBy("REFR_NO", "PAT_ID", "BTYPE_CD")
        .agg(F.sum("PMNT_AMT").cast("decimal(18,2)").alias("valor_pago_paciente")))

# fato_faturamento: faturas de atendimento (RG) + de medicamentos (MD)
fat = (s("bill").unionByName(s("mdbl"), allowMissingColumns=True)
       .join(pago, ["REFR_NO", "PAT_ID", "BTYPE_CD"], "left")
       .select(sk("REFR_NO", "PAT_ID", "BTYPE_CD").alias("sk_fatura"),
               sk("REFR_NO", "PAT_ID").alias("sk_visita"), F.col("REFR_NO").alias("num_fatura"),
               sk("PAT_ID").alias("sk_paciente"), data_key("BILL_DATE").alias("data_emissao_key"),
               F.when(F.col("BTYPE_CD") == "RG", "Services").otherwise("Medications").alias("tipo_fatura"),
               F.col("BILL_AMT").alias("valor_fatura"), F.col("BSTAT_DES").alias("status"),
               F.coalesce("valor_pago_paciente", F.lit(0).cast("decimal(18,2)")).alias("valor_pago_paciente")))
gravar(fat, "fato_faturamento")

# fato_pagamento: 1 linha por parcela paga pelo paciente
pag = s("pmnt").select(
    sk("REFR_NO", "PAT_ID", "BTYPE_CD").alias("sk_fatura"),
    sk("REFR_NO", "PAT_ID").alias("sk_visita"), sk("PAT_ID").alias("sk_paciente"),
    data_key("PMNT_DATE").alias("data_pagamento_key"),
    F.when(F.col("BTYPE_CD") == "RG", "Atendimento").otherwise("Medicamentos").alias("tipo_fatura"),
    F.col("PMNT_NO").alias("parcela"), F.col("PMNT_TMS").alias("total_parcelas"),
    F.col("TOT_PMNT").alias("valor_total_paciente"), F.col("PMNT_AMT").alias("valor_parcela"),
    F.col("METH_DES").alias("meio_pagamento"), F.col("PSTAT_DES").alias("status"))
gravar(pag, "fato_pagamento")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Fatos de suprimentos

# CELL ********************

# fato_fornecimento: 1 linha por lote recebido, com o resumo do pagamento ao fornecedor
pg_forn = s("vpmt").groupBy("SPL_ID").agg(
    F.max("SPL_CST").alias("custo_total"),
    F.sum("VPMT_AMT").cast("decimal(18,2)").alias("valor_pago"),
    F.min("PMNT_DUE").alias("saldo_a_pagar"),
    F.count("*").alias("parcelas_pagas"),
    F.max("PMNT_TRM").alias("parcelas_combinadas"))
fornec = (s("sply").join(pg_forn, "SPL_ID", "left")
          .select(F.col("SPL_ID").alias("num_fornecimento"), sk("MAT_SKU").alias("sk_material"),
                  sk("VEN_ACC").alias("sk_fornecedor"), sk("RCVD_STF").alias("sk_recebedor"),
                  F.col("MAT_TIPO").alias("tipo_material"),
                  data_key("MFG_DATE").alias("data_fabricacao_key"),
                  data_key("RCVD_DATE").alias("data_recebimento_key"),
                  data_key("VRFC_DATE").alias("data_conferencia_key"),
                  F.col("SPL_QTY").alias("qtd_fornecida"), F.col("RCVD_QTY").alias("qtd_recebida"),
                  (F.col("SPL_QTY") - F.col("RCVD_QTY")).alias("qtd_divergente"),
                  F.col("SPL_UOM").alias("unidade"), F.col("SPL_UCT").alias("custo_unitario"),
                  F.datediff("VRFC_DATE", "RCVD_DATE").alias("dias_ate_conferencia"),
                  "custo_total", "valor_pago", "saldo_a_pagar", "parcelas_pagas", "parcelas_combinadas"))
gravar(fornec, "fato_fornecimento")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fato_estoque_mensal: 1 linha por medicamento + mês (resumo dos registros diários)
inv = s("invn").withColumn("mes", F.trunc("INGTN_TS", "month"))
est = (inv.groupBy("MAT_SKU", "mes").agg(
           F.min_by("OPEN_QTY", "INGTN_TS").alias("estoque_inicial"),
           F.max_by("CLSD_QTY", "INGTN_TS").alias("estoque_final"),
           F.sum("RCVD_QTY").alias("qtd_recebida"),
           F.sum("ISSD_QTY").alias("qtd_saida"),
           F.count("*").alias("dias_com_registro"),
           F.sum(F.when(F.col("CLSD_QTY") <= 0, 1).otherwise(0)).alias("dias_em_ruptura"))
       .select(sk("MAT_SKU").alias("sk_medicamento"), data_key("mes").alias("mes_key"),
               "estoque_inicial", "estoque_final", "qtd_recebida", "qtd_saida",
               "dias_com_registro", "dias_em_ruptura",
               (F.col("estoque_final") < 0).alias("fechou_negativo")))
gravar(est, "fato_estoque_mensal")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Conferência
# 
# **Integridade:** para cada chave dos fatos, conta quantos valores não existem na dimensão correspondente. O esperado é **0** em todas as linhas: valores ausentes apontam para o membro `-1` (Não informado), que existe em todas as dimensões.
# 
# **Visitas ambíguas:** mostra quantas linhas de diagnóstico, tratamento e prescrição ficaram de fora por pertencerem a visitas com `REFR_NO` ambíguo.

# CELL ********************

RELACOES = [
    ("fato_agendamento", "sk_paciente", "dim_paciente"), ("fato_agendamento", "sk_medico", "dim_funcionario"),
    ("fato_agendamento", "sk_atendente", "dim_funcionario"), ("fato_agendamento", "data_agendamento_key", "dim_calendario"),
    ("fato_visita", "sk_paciente", "dim_paciente"), ("fato_visita", "sk_sala", "dim_sala"),
    ("fato_visita", "data_entrada_key", "dim_calendario"), ("fato_visita", "data_saida_key", "dim_calendario"),
    ("fato_diagnostico", "sk_paciente", "dim_paciente"), ("fato_diagnostico", "sk_sala", "dim_sala"),
    ("fato_diagnostico", "data_key", "dim_calendario"), ("fato_diagnostico", "sk_visita", "fato_visita"),
    ("fato_tratamento", "sk_paciente", "dim_paciente"), ("fato_tratamento", "data_key", "dim_calendario"),
    ("fato_prescricao", "sk_paciente", "dim_paciente"), ("fato_prescricao", "sk_medicamento", "dim_medicamento"),
    ("fato_prescricao", "data_key", "dim_calendario"),
    ("fato_faturamento", "sk_paciente", "dim_paciente"), ("fato_faturamento", "data_emissao_key", "dim_calendario"),
    ("fato_faturamento", "sk_visita", "fato_visita"),
    ("fato_pagamento", "sk_paciente", "dim_paciente"), ("fato_pagamento", "data_pagamento_key", "dim_calendario"),
    ("fato_pagamento", "sk_fatura", "fato_faturamento"),
    ("fato_fornecimento", "sk_material", "dim_material"), ("fato_fornecimento", "sk_fornecedor", "dim_fornecedor"),
    ("fato_fornecimento", "sk_recebedor", "dim_funcionario"), ("fato_fornecimento", "data_recebimento_key", "dim_calendario"),
    ("fato_estoque_mensal", "sk_medicamento", "dim_medicamento"), ("fato_estoque_mensal", "mes_key", "dim_calendario"),
]
# Coluna-chave de cada tabela de destino
CHAVE = {"dim_paciente": "sk_paciente", "dim_funcionario": "sk_funcionario", "dim_sala": "sk_sala",
         "dim_medicamento": "sk_medicamento", "dim_material": "sk_material", "dim_fornecedor": "sk_fornecedor",
         "dim_calendario": "data_key", "fato_visita": "sk_visita", "fato_faturamento": "sk_fatura"}

g = lambda t: spark.table(f"{GOLD}.{t}")
res = []
for fato, col, dim in RELACOES:
    chave = CHAVE[dim]
    sem_par = (g(fato).filter(F.col(col) != -1).select(col).distinct()
               .join(g(dim).select(F.col(chave).alias(col)), col, "left_anti").count())
    res.append((fato, col, dim, sem_par))
display(spark.createDataFrame(res, "fato string, coluna string, destino string, valores_sem_correspondencia long"))

amb = [(t, s(t).filter(F.col("REFR_AMBIGUO")).count()) for t in ["diag", "trtm", "tmmd"]]
display(spark.createDataFrame(amb, "tabela_silver string, linhas_fora_da_gold long"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Interrompe o pipeline se algum fato apontar para chave inexistente
problemas = [r for r in res if r[3] > 0]
if problemas:
    raise Exception(f"Relações quebradas: {problemas}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
