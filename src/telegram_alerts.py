# src/telegram_alerts.py
import requests
import pandas as pd 
import os
import json
from datetime import datetime as dt 

# --- Configurações de Estado ---
SENT_ALERTS_PATH = "data/sent_alerts.json"

# --- Funções de Suporte ao Estado ---

def get_game_id(row):
    """Cria um ID único para o jogo (País + Times + Horário)"""
    # Usamos Horário para tornar o ID mais único, caso os times se repitam em dias diferentes
    # O Horário deve ser convertido para string H:M para o ID
    horario_str = str(row.get('Horário', '00:00')).split(' ')[-1][:5]
    return f"{row.get('País', 'NP')}-{row.get('Time 1', 'NT1')}-vs-{row.get('Time 2', 'NT2')}-{horario_str}"

def load_sent_alerts():
    """Carrega IDs de jogos já alertados."""
    if os.path.exists(SENT_ALERTS_PATH):
        try:
            with open(SENT_ALERTS_PATH, 'r') as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()

def save_sent_alerts(sent_alerts_set):
    """Salva IDs de jogos já alertados."""
    if not os.path.exists('data'): 
        os.makedirs('data')
    with open(SENT_ALERTS_PATH, 'w') as f:
        json.dump(list(sent_alerts_set), f, indent=4)

# --- Função de Envio Genérica ---

def enviar_mensagem(chat_id, mensagem, token):
    """Envia a mensagem via Telegram API."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": mensagem,
        "parse_mode": "HTML" 
    }
    try:
        response = requests.post(url, data=payload)
        if response.status_code != 200:
            print(f"Erro {response.status_code} ao enviar para {chat_id}. Resposta: {response.text}")
    except Exception as e:
        print(f"Erro ao enviar mensagem: {e}")

# --- Função de Formatação Detalhada ---

def formatar_mensagem_alerta(row):
    """
    Formata a mensagem do Telegram com as probabilidades detalhadas.
    Aplica horário corrigido (se já enviado pelo main) e mostra HH:MM local.
    """
    import urllib.parse
    import pandas as pd

    tipo = row.get('Tipo_Alerta', 'ALERTA_PADRÃO')

    if tipo == "HIGH_PROB":
        header = "🚀 <b>ALERTA PREMIUM (HIGH PROB)</b> 🚀"
    elif tipo == "ALERTA_30MIN":
        header = "🔔 <b>ALERTA DE JOGO PRÓXIMO (30 MIN)</b> 🔔"
    elif tipo == "ALERTA_120MIN":
        header = "🔔 <b>ALERTA PRÉ-JOGO (até 2h)</b> 🔔"
    else:
        header = "⚽ <b>NOVO ALERTA DE JOGO</b> ⚽"

    time1 = str(row.get('Time 1', 'N/A'))
    time2 = str(row.get('Time 2', 'N/A'))
    pais = row.get('País', 'N/A')
    # Usa o horário já corrigido pelo main.py, se presente
    horario = row.get('Horário', 'N/A')
    liga = row.get('LIGA', None)
    partidas = row.get('Partidas', 'N/A')
    media_prob = row.get('MÉDIA_PROB', 0)

    query = urllib.parse.quote(f"{time1} vs {time2}")
    link_google = f'<a href="https://www.google.com/search?q={query}" target="_blank">🔎 Ver jogo</a>'

    def fmt_pct(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "N/A"
        try:
            return f"{float(val):.0f}%"
        except Exception:
            return "N/A"

    def fmt_num(val, nd=2):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "N/A"
        try:
            return f"{float(val):.{nd}f}"
        except Exception:
            return "N/A"

    over15_media = row.get('Prob_Over1.5', row.get('Over15_MEDIA', 0))
    over25_media = row.get('Prob_Over2.5', row.get('Over25_MEDIA', 0))
    over_both = row.get('Over_BOTH', 0)

    over15_h = row.get('Over15_H', 0)
    over25_h = row.get('Over25_H', 0)
    btts_h = row.get('BTTS_H', 0)
    over15_a = row.get('Over15_A', 0)
    over25_a = row.get('Over25_A', 0)
    btts_a = row.get('BTTS_A', 0)

    ppg_h = row.get('PPG_Casa', 0)
    ppg_a = row.get('PPG_Fora', 0)
    media_gols_h = row.get('Media_Gols_Casa', 0)
    media_gols_a = row.get('MediaGols_Fora', 0)

    gm_h = row.get('Gols_Marcados_Casa', 0)
    gs_h = row.get('Gols_Sofridos_Casa', 0)
    gm_a = row.get('Gols_Marcados_Fora', 0)
    gs_a = row.get('Gols_Sofridos_Fora', 0)

    vitorias_h = row.get('Vitorias_H', row.get('%Vitorias_H', None))
    vitorias_a = row.get('Vitorias_A', row.get('%Vitorias_A', None))

    mensagem = (
        f"{header}\n"
        f"<b>{time1}</b> vs <b>{time2}</b> | {link_google}\n"
        f"País: {pais}" + (f" | Liga: {liga}\n" if liga else "\n") +
        f"Horário: {horario}\n\n"
        f"🔥 <b>MÉDIA GERAL:</b> {fmt_pct(media_prob)}\n"
        f"✅ <b>Partidas Analisadas:</b> {fmt_num(partidas, nd=0)}\n\n"
        f"📊 <b>Probabilidades (Médias):</b>\n"
        f"• Over 1.5: {fmt_pct(over15_media)}\n"
        f"• Over 2.5: {fmt_pct(over25_media)}\n"
        f"• Ambas/Over Total: {fmt_pct(over_both)}\n\n"
        f"🏠 <b>{time1} (H) | {time2} (A)</b>\n"
        f"O1.5: {fmt_pct(over15_h)} | {fmt_pct(over15_a)}\n"
        f"O2.5: {fmt_pct(over25_h)} | {fmt_pct(over25_a)}\n"
        f"BTTS: {fmt_pct(btts_h)} | {fmt_pct(btts_a)}\n\n"
        f"📈 <b>PPG</b>: {fmt_num(ppg_h, nd=2)} | {fmt_num(ppg_a, nd=2)}\n"
        f"⚽ <b>Média de Gols</b>: {fmt_num(media_gols_h, nd=2)} | {fmt_num(media_gols_a, nd=2)}\n"
        f"🎯 <b>Gols Marcados/Sofridos</b> (H): {fmt_num(gm_h, nd=0)}/{fmt_num(gs_h, nd=0)} | (A): {fmt_num(gm_a, nd=0)}/{fmt_num(gs_a, nd=0)}\n"
    )
    if vitorias_h is not None or vitorias_a is not None:
        mensagem += f"🏆 <b>%Vitórias</b>: {fmt_pct(vitorias_h)} | {fmt_pct(vitorias_a)}\n"

    return mensagem

# --- Função de Envio Único (A ser chamada pelo main.py e app.py) ---

def enviar_alertes_unicos(df_com_filtros_aplicados, token, usuarios):
    """
    Filtra o DF de alertas, enviando apenas os jogos que ainda não foram alertados,
    e usa a formatação detalhada.
    """
    
    # 1. Carrega os alertas já enviados
    sent_alerts = load_sent_alerts()
    
    # 2. Prepara o ID de cada jogo para verificação
    df_com_filtros_aplicados['game_id'] = df_com_filtros_aplicados.apply(get_game_id, axis=1)
    
    # Filtra apenas os jogos que AINDA NÃO foram enviados
    df_novos_alertas = df_com_filtros_aplicados[~df_com_filtros_aplicados['game_id'].isin(sent_alerts)].copy()
    
    # 3. Se houver novos alertas, envia
    if not df_novos_alertas.empty:
        
        newly_sent_ids = set()
        
        for _, row in df_novos_alertas.iterrows():
            
            # Monta a mensagem usando a função detalhada
            mensagem = formatar_mensagem_alerta(row)
            
            # Envia para todos os usuários
            for user_id in usuarios:
                enviar_mensagem(user_id, mensagem, token)
            
            # Adiciona o ID para a atualização
            newly_sent_ids.add(row['game_id'])
        
        # 4. Atualiza o registro de alertas enviados
        sent_alerts.update(newly_sent_ids)
        save_sent_alerts(sent_alerts)
        
        return df_novos_alertas
        
    return pd.DataFrame()