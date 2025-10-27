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
    Esta é a parte que garante a riqueza das informações.
    """
    
    tipo = row.get('Tipo_Alerta', 'ALERTA_PADRÃO')
    
    # === HEADER ===
    if tipo == "HIGH_PROB":
        header = "🚀 <b>ALERTA PREMIUM (HIGH PROB)</b> 🚀"
    elif tipo == "ALERTA_30MIN":
        header = "🔔 <b>ALERTA DE JOGO PRÓXIMO (30 MIN)</b> 🔔"
    else:
        header = "⚽ <b>NOVO ALERTA DE JOGO</b> ⚽"
        
    # === CORPO BÁSICO ===
    mensagem = (
        f"{header}\n"
        f"<b>{row.get('Time 1', 'N/A')}</b> vs <b>{row.get('Time 2', 'N/A')}</b>\n"
        f"País: {row.get('País', 'N/A')}\n"
        f"Horário: {row.get('Horário', 'N/A')}\n\n"
        f"🔥 <b>MÉDIA GERAL: {row.get('MÉDIA_PROB', 0):.0f}%</b>\n"
        f"✅ <b>Partidas Analisadas: {row.get('Partidas', 'N/A'):.0f}</b>\n\n"
    )
    
    # === DETALHES DAS PROBABILIDADES ===
    
    # Usando .get() e round(0) para as médias calculadas
    over15_media = row.get('Over15_MEDIA', 0)
    over25_media = row.get('Over25_MEDIA', 0)
    over_both = row.get('Over_BOTH', 0)
    
    mensagem += (
        f"📊 <b>Probabilidades de Média:</b>\n"
        f"• Over 1.5: {over15_media:.0f}%\n"
        f"• Over 2.5: {over25_media:.0f}%\n"
        f"• Ambas Marcam/Over Total: {over_both:.0f}%\n\n"
    )
    
    # === DETALHES DA DIVISÃO (CASA/FORA) ===
    
    # Usando .get() e round(0) para as colunas originais
    over15_h = row.get('Over15_H', 0)
    over25_h = row.get('Over25_H', 0)
    btts_h = row.get('BTTS_H', 0)
    
    over15_a = row.get('Over15_A', 0)
    over25_a = row.get('Over25_A', 0)
    btts_a = row.get('BTTS_A', 0)
    
    mensagem += (
        f"🏠 <b>{row.get('Time 1', 'Casa')} (H) | {row.get('Time 2', 'Fora')} (A)</b>\n"
        f"O1.5: {over15_h:.0f}% | {over15_a:.0f}%\n"
        f"O2.5: {over25_h:.0f}% | {over25_a:.0f}%\n"
        f"BTTS: {btts_h:.0f}% | {btts_a:.0f}%\n"
    )

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