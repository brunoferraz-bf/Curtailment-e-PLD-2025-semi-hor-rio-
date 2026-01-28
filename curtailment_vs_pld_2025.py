"""
Estudo: Curtailment vs PLD (2025)

Objetivo:
- Tratar dados oficiais do ONS (restrições de geração eólica e solar)
- Calcular curtailment em base semi-horária
- Integrar com PLD horário convertido para semi-horário
- Valorar o curtailment ao PLD

Hipótese:
- As usinas são consideradas como contratadas no mercado livre
- A energia não gerada por curtailment precisaria ser comprada no MCP
"""

import pandas as pd
import numpy as np
from pathlib import Path

# =========================
# DEFINIÇÃO DE CAMINHOS
# =========================
BASE_PATH = Path("COLOCAR O ENDEREÇO DO SEU REPOSITÓRIO")

pld_path = BASE_PATH / "pld_horario_2025.csv" # https://pda-download.ccee.org.br/korJMXwpSLGyVlpRMQWduA/content

# =========================
# LEITURA E TRATAMENTO DO PLD HORÁRIO
# =========================
df_pld = pd.read_csv(pld_path, sep=";")

# Extração de ano e mês a partir do campo MES_REFERENCIA (formato AAAAMM)
df_pld["ano"] = df_pld["MES_REFERENCIA"] // 100
df_pld["mes"] = df_pld["MES_REFERENCIA"] % 100

# Construção do timestamp horário
df_pld["din_instante"] = pd.to_datetime(
    dict(
        year=df_pld["ano"],
        month=df_pld["mes"],
        day=df_pld["DIA"],
        hour=df_pld["HORA"]
    )
)

# Seleção e padronização de colunas
df_pld = df_pld[["din_instante", "SUBMERCADO", "PLD_HORA"]]
df_pld = df_pld.rename(columns={"SUBMERCADO": "id_subsistema"})

# Padronização dos nomes dos subsistemas
mapa = {
    "NORTE": "N",
    "NORDESTE": "NE",
    "SUL": "S",
    "SUDESTE": "SE"
}
df_pld["id_subsistema"] = df_pld["id_subsistema"].replace(mapa)

# =========================
# CONVERSÃO DE PLD HORÁRIO → SEMI-HORÁRIO
# =========================
df_pld = df_pld.sort_values("din_instante")

# Duplica cada hora em dois registros de 30 minutos
df_pld_sh = df_pld.loc[df_pld.index.repeat(2)].copy()
df_pld_sh["offset_min"] = df_pld_sh.groupby(level=0).cumcount() * 30

df_pld_sh["din_instante"] = (
    df_pld_sh["din_instante"] +
    pd.to_timedelta(df_pld_sh["offset_min"], unit="m")
)

df_pld_sh = df_pld_sh.drop(columns="offset_min").reset_index(drop=True)
df_pld_sh = df_pld_sh.rename(columns={"PLD_HORA": "PLD_sh"})

# =========================
# LEITURA DOS ARQUIVOS ONS – EÓLICA # https://dados.ons.org.br/dataset/restricao_coff_eolica_detail
# =========================
arquivos_eolica = sorted(BASE_PATH.glob("RESTRICAO_COFF_EOLICA_2025_*.parquet"))

dfs_eolica = [pd.read_parquet(arq) for arq in arquivos_eolica]
df_eolica = pd.concat(dfs_eolica, ignore_index=True)

df_eolica["din_instante"] = pd.to_datetime(df_eolica["din_instante"])

# Conversão de colunas numéricas
cols_energia = [
    "val_geracao",
    "val_geracaolimitada",
    "val_disponibilidade",
    "val_geracaoreferencia",
    "val_geracaoreferenciafinal"
]

for col in cols_energia:
    df_eolica[col] = pd.to_numeric(df_eolica[col], errors="coerce")

# Cálculo do curtailment semi-horário
df_eolica["curtailment_sh"] = np.where(
    df_eolica["val_geracaolimitada"].notna() &
    (df_eolica["val_geracaoreferencia"] > df_eolica["val_geracao"]),
    df_eolica["val_geracaoreferencia"] - df_eolica["val_geracao"],
    0.0
)

df_eolica = df_eolica[
    ["din_instante", "id_subsistema", "nom_usina", "cod_razaorestricao", "curtailment_sh"]
].copy()

df_eolica.columns = ["din_instante", "id_subsistema", "USINA", "RAZAO", "CURTAILMENT"]
df_eolica["FONTE"] = "EOLICA"

# =========================
# LEITURA DOS ARQUIVOS ONS – SOLAR # https://dados.ons.org.br/dataset/restricao_coff_fotovoltaica_detail
# =========================
arquivos_fotovoltaica = sorted(BASE_PATH.glob("RESTRICAO_COFF_FOTOVOLTAICA_2025_*.parquet"))

dfs_fotovoltaica = [pd.read_parquet(arq) for arq in arquivos_fotovoltaica]
df_fotovoltaica = pd.concat(dfs_fotovoltaica, ignore_index=True)

df_fotovoltaica["din_instante"] = pd.to_datetime(df_fotovoltaica["din_instante"])

for col in cols_energia:
    df_fotovoltaica[col] = pd.to_numeric(df_fotovoltaica[col], errors="coerce")

df_fotovoltaica["curtailment_sh"] = np.where(
    df_fotovoltaica["val_geracaolimitada"].notna() &
    (df_fotovoltaica["val_geracaoreferencia"] > df_fotovoltaica["val_geracao"]),
    df_fotovoltaica["val_geracaoreferencia"] - df_fotovoltaica["val_geracao"],
    0.0
)

df_fotovoltaica = df_fotovoltaica[
    ["din_instante", "id_subsistema", "nom_usina", "cod_razaorestricao", "curtailment_sh"]
].copy()

df_fotovoltaica.columns = ["din_instante", "id_subsistema", "USINA", "RAZAO", "CURTAILMENT"]
df_fotovoltaica["FONTE"] = "SOLAR"

# =========================
# CONSOLIDAÇÃO E VALORAÇÃO
# =========================
df_curtailment = pd.concat(
    [df_eolica, df_fotovoltaica],
    ignore_index=True
)

# Merge com PLD semi-horário
df_final = df_curtailment.merge(
    df_pld_sh,
    on=["din_instante", "id_subsistema"],
    how="left"
)

# Conversão MWmed → MWh (base semi-horária)
df_final["CURTAILMENT_MWh"] = df_final["CURTAILMENT"] * 0.5

# Valoração ao PLD
df_final["CURTAILMENT_R$"] = (
    df_final["CURTAILMENT_MWh"] * df_final["PLD_sh"]
)

df_final
