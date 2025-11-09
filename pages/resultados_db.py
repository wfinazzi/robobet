# Resultados (Banco de Dados)
import os
import streamlit as st
import pandas as pd
from datetime import datetime
import pytz
from src.database import get_mysql_connection

st.set_page_config(page_title="Resultados (DB)", layout="wide")
st.sidebar.title("Menu")
st.sidebar.markdown("- Página: Raspagem e Análise")
st.sidebar.markdown("- Página: Resultados (DB)")

# Usa .env diretamente (sem inputs)
mysql_host = os.getenv("MYSQL_HOST", "localhost")
mysql_user = os.getenv("MYSQL_USER", "")
mysql_password = os.getenv("MYSQL_PASSWORD", "")
mysql_db = os.getenv("MYSQL_DB", "simulador-apostas")

st.title("📓 Resultados (DB)")

tz_target = pytz.timezone("America/Sao_Paulo")
data_default = datetime.now(tz_target).date()

# --- Filtros: intervalo de datas ---
st.subheader("Filtros")
col_dt1, col_dt2 = st.columns(2)
with col_dt1:
    data_inicio = st.date_input("Data inicial", value=data_default)
with col_dt2:
    data_fim = st.date_input("Data final", value=data_default)

st.markdown("---")
st.subheader(f"Resultados em '{mysql_db}' de {data_inicio} até {data_fim}")

# --- Consulta no DB pelo intervalo de datas ---
df_resultados = pd.DataFrame()
try:
    conn = get_mysql_connection()
    query = """
        SELECT *
        FROM jogos
        WHERE DATA_JOGO BETWEEN %s AND %s
          AND GOLS_CASA IS NOT NULL
          AND GOLS_FORA IS NOT NULL
    """
    df_resultados = pd.read_sql(query, conn, params=[data_inicio, data_fim])
    conn.close()
except Exception as e:
    st.error(f"Erro ao consultar o DB: {e}")

# Refina novamente no DF (robustez)
if not df_resultados.empty:
    if {'GOLS_CASA', 'GOLS_FORA'}.issubset(df_resultados.columns):
        df_resultados = df_resultados.dropna(subset=['GOLS_CASA', 'GOLS_FORA'])
if df_resultados.empty:
    st.info("Nenhum jogo encontrado para o intervalo de datas selecionado.")
else:
    cols_existentes = set(df_resultados.columns)

    # --- Filtros integrados: País -> Liga (selects dependentes) ---
    st.subheader("Filtros por País e Liga")

    # País
    if "PAIS" in cols_existentes:
        paises = sorted(df_resultados["PAIS"].dropna().unique().tolist())
        pais_opcoes = ["Todos"] + paises if paises else ["Todos"]
        pais_sel = st.selectbox("País", options=pais_opcoes, index=0)
    else:
        pais_sel = "Todos"

    # Liga depende do País selecionado
    if "LIGA" in cols_existentes:
        if pais_sel == "Todos" or "PAIS" not in cols_existentes:
            ligas_pool = df_resultados["LIGA"]
        else:
            ligas_pool = df_resultados.loc[df_resultados["PAIS"] == pais_sel, "LIGA"]
        ligas = sorted(ligas_pool.dropna().unique().tolist())
        liga_opcoes = ["Todas"] + ligas if ligas else ["Todas"]
        liga_sel = st.selectbox("Liga", options=liga_opcoes, index=0)
    else:
        liga_sel = "Todas"

    # Aplica filtros
    df_filtrado = df_resultados.copy()
    if pais_sel != "Todos" and "PAIS" in cols_existentes:
        df_filtrado = df_filtrado[df_filtrado["PAIS"] == pais_sel]
    if liga_sel != "Todas" and "LIGA" in cols_existentes:
        df_filtrado = df_filtrado[df_filtrado["LIGA"] == liga_sel]

    # Mantém somente partidas com gols preenchidos (reforço de segurança)
    if {'GOLS_CASA', 'GOLS_FORA'}.issubset(df_filtrado.columns):
        df_filtrado = df_filtrado.dropna(subset=['GOLS_CASA', 'GOLS_FORA'])

    # Garante df_stats sempre definido
    df_stats = pd.DataFrame()
    if {'GOLS_CASA', 'GOLS_FORA'}.issubset(df_filtrado.columns):
        df_stats = df_filtrado.dropna(subset=['GOLS_CASA', 'GOLS_FORA']).copy()

    # --- Cards de resumo: Número de partidas e Rodadas (média) ---
    partidas_total = len(df_filtrado)
    rodadas_media = 0.0
    if {'CONT_HOME', 'CONT_AWAY'}.issubset(df_filtrado.columns) and partidas_total > 0:
        cont_home = pd.to_numeric(df_filtrado['CONT_HOME'], errors='coerce').fillna(0)
        cont_away = pd.to_numeric(df_filtrado['CONT_AWAY'], errors='coerce').fillna(0)
        media_cont_por_jogo = (cont_home + cont_away) / 2.0
        rodadas_media = float(media_cont_por_jogo.mean())

    row0 = st.columns(2)
    with row0[0]:
        st.metric("Número de Partidas", f"{partidas_total}")
    with row0[1]:
        st.metric("Rodadas (média)", f"{rodadas_media:.1f}")

    # --- Estatísticas (%) ---
    st.markdown("---")
    st.subheader("Estatísticas (%)")
    total = len(df_stats)
    if total == 0:
        st.info("Sem partidas com gols preenchidos na seleção atual.")
        total_goals = pd.Series(dtype=float)
        home_wins = draws = away_wins = 0
        over_0_5 = over_1_5 = over_2_5 = over_3_5 = btts = 0
    else:
        total_goals = pd.to_numeric(df_stats["GOLS_CASA"], errors="coerce").fillna(0) + \
                      pd.to_numeric(df_stats["GOLS_FORA"], errors="coerce").fillna(0)
        home_wins = (df_stats["GOLS_CASA"] > df_stats["GOLS_FORA"]).sum()
        draws = (df_stats["GOLS_CASA"] == df_stats["GOLS_FORA"]).sum()
        away_wins = (df_stats["GOLS_FORA"] > df_stats["GOLS_CASA"]).sum()
        over_0_5 = (total_goals >= 1).sum()
        over_1_5 = (total_goals >= 2).sum()
        over_2_5 = (total_goals >= 3).sum()
        over_3_5 = (total_goals >= 4).sum()
        btts = ((pd.to_numeric(df_stats["GOLS_CASA"], errors="coerce").fillna(0) >= 1) &
                (pd.to_numeric(df_stats["GOLS_FORA"], errors="coerce").fillna(0) >= 1)).sum()

    def pct(x):
        return f"{(0 if total == 0 else (x / total * 100)):.0f}%"

    row1 = st.columns(4)
    with row1[0]:
        st.metric("Mandantes Vencendo", pct(home_wins))
    with row1[1]:
        st.metric("Empates", pct(draws))
    with row1[2]:
        st.metric("Visitantes Vencendo", pct(away_wins))
    with row1[3]:
        st.metric("+0,5 gols", pct(over_0_5))

    row2 = st.columns(4)
    with row2[0]:
        st.metric("+1,5 gols", pct(over_1_5))
    with row2[1]:
        st.metric("+2,5 gols", pct(over_2_5))
    with row2[2]:
        st.metric("+3,5 gols", pct(over_3_5))
    with row2[3]:
        st.metric("Ambas Marcam", pct(btts))

    # --- Painel de médias (mantém e reexibe estatísticas anteriores) ---
    st.markdown("---")
    st.subheader("Médias de Probabilidades e Métricas (anteriores)")
    def mean_pct(col):
        if col in df_filtrado.columns:
            vals = pd.to_numeric(df_filtrado[col], errors='coerce')
            if vals.notna().any():
                return f"{vals.dropna().mean():.0f}%"
        return "N/A"

    def mean_num(col):
        if col in df_filtrado.columns:
            vals = pd.to_numeric(df_filtrado[col], errors='coerce')
            if vals.notna().any():
                return f"{vals.dropna().mean():.2f}"
        return "N/A"

    row_prev1 = st.columns(4)
    with row_prev1[0]:
        st.metric("Média Over 1.5", mean_pct("PROB_OVER_1_5"))
    with row_prev1[1]:
        st.metric("Média Over 2.5", mean_pct("PROB_OVER_2_5"))
    with row_prev1[2]:
        st.metric("Média BTTS", mean_pct("PROB_BTTS"))
    with row_prev1[3]:
        st.metric("Média Geral (MEDIA_PROB)", mean_num("MEDIA_PROB"))

    row_prev2 = st.columns(4)
    with row_prev2[0]:
        st.metric("Média Media Home", mean_num("MEDIA_HOME"))
    with row_prev2[1]:
        st.metric("Média Media Away", mean_num("MEDIA_AWAY"))
    with row_prev2[2]:
        st.metric("Média Cont Home", mean_num("CONT_HOME"))
    with row_prev2[3]:
        st.metric("Média Cont Away", mean_num("CONT_AWAY"))

    # --- Tabela amigável (ordenada) ---
    st.markdown("---")
    st.subheader(f"Partidas encontradas: {len(df_filtrado)}")
    if "MEDIA_PROB" in df_filtrado.columns:
        df_filtrado = df_filtrado.sort_values("MEDIA_PROB", ascending=False)

    cols_display = [
        "DATA_JOGO", "PAIS", "LIGA", "TIME_CASA", "TIME_FORA",
        "GOLS_CASA", "GOLS_FORA",
        "PROB_OVER_1_5", "PROB_OVER_2_5", "PROB_BTTS", "MEDIA_PROB",
        "MEDIA_HOME", "MEDIA_AWAY", "CONT_HOME", "CONT_AWAY"
    ]
    cols_display = [c for c in cols_display if c in df_filtrado.columns]

    st.dataframe(
        df_filtrado[cols_display].round(2),
        use_container_width=True,
        hide_index=True,
    )

# Teste rápido de conexão (sem inputs)
st.markdown("---")
st.subheader("Conexão MySQL")
if st.button("🔌 Testar conexão"):
    try:
        conn = get_mysql_connection()
        conn.close()
        st.success("Conexão MySQL bem-sucedida!")
    except Exception as e:
        st.error(f"Erro ao conectar no MySQL: {e}")