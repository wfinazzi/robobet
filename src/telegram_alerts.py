# telegram_alerts.py
import requests
import datetime
import pandas as pd 
from datetime import datetime as dt # Alias para datetime.datetime

# --- Função para enviar mensagem para um chat específico ---
def enviar_mensagem(chat_id, mensagem, token):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": mensagem,
        "parse_mode": "HTML" 
    }
    try:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            print(f"Mensagem enviada para {chat_id}")
        else:
            print(f"Erro {response.status_code} ao enviar para {chat_id}. Resposta: {response.text}")
    except Exception as e:
        print(f"Erro ao enviar mensagem: {e}")

# --- Função para enviar alertas de jogos próximos (EXISTENTE - INTACTA) ---
def enviar_alertas(df, token, usuarios, minutos_antes=30):
    """
    Função genérica chamada pelo loop principal e pelo botão de teste no Streamlit.
    Assume que o DF já está filtrado e pronto para envio.
    """
    agora = dt.now()
    
    for _, jogo in df.iterrows():
        # Tenta formatar as probabilidades com 1 casa decimal
        prob_1_5 = jogo.get('Prob_Over1.5', jogo.get('Over15_MEDIA', 'N/A'))
        prob_2_5 = jogo.get('Prob_Over2.5', jogo.get('Over25_MEDIA', 'N/A'))
        prob_btts = jogo.get('Prob_BTTS', jogo.get('Over_BOTH', 'N/A'))
        
        # Garante que os números sejam exibidos formatados se forem numéricos
        if isinstance(prob_1_5, (int, float)): prob_1_5 = f"{prob_1_5:.0f}"
        if isinstance(prob_2_5, (int, float)): prob_2_5 = f"{prob_2_5:.0f}"
        if isinstance(prob_btts, (int, float)): prob_btts = f"{prob_btts:.0f}"
        
        mensagem = (
            f"⚽ <b>{jogo.get('Time 1', 'N/A')}</b> x <b>{jogo.get('Time 2', 'N/A')}</b>\n"
            f"País: {jogo.get('País', 'N/A')}\n"
            f"Horário: {jogo.get('Horário', 'N/A')}\n"
            f"📊 Probabilidades:\n"
            f"• Over 1.5: {prob_1_5}%\n" 
            f"• Over 2.5: {prob_2_5}%\n" 
            f"• Ambas Marcam (BTTS): {prob_btts}%\n" 
            f"Média Probabilidade: {jogo.get('MÉDIA_PROB', 'N/A')}%\n" 
            f"Tipo de aposta sugerido: {jogo.get('Tipo_Aposta', 'Não definido')}"
        )
        
        for chat_id in usuarios:
            enviar_mensagem(chat_id, mensagem, token)

# --- Função para alertas de Alta Probabilidade ---
def enviar_alerta_high_prob(df, token, usuarios, min_prob=70, min_partidas=10):
    """
    Filtra e envia alertas para jogos com MÉDIA_PROB acima de 70% e Partidas acima de 10.
    """
    df_top = df.copy()
    
    # 1. Aplica a lógica de filtro (Garantiu que MÉDIA_PROB e Partidas já são numéricos no main.py)
    if 'MÉDIA_PROB' in df_top.columns and 'Partidas' in df_top.columns:
        df_top = df_top[
            (df_top['MÉDIA_PROB'] >= min_prob) & 
            (df_top['Partidas'] >= min_partidas)
        ]
        df_top.dropna(subset=['MÉDIA_PROB', 'Partidas'], inplace=True)
    else:
        print("Erro: Colunas 'MÉDIA_PROB' ou 'Partidas' não encontradas ou não são numéricas para HIGH PROB.")
        return

    if df_top.empty:
        # print(f"[{dt.now().strftime('%H:%M:%S')}] Nenhum jogo de Alta Probabilidade encontrado.")
        return
        
    for _, jogo in df_top.iterrows():
        mensagem = (
            f"🚀 <b>ALERTA PREMIUM (HIGH PROB)</b> 🚀\n"
            f"<b>{jogo.get('Time 1', 'N/A')}</b> x <b>{jogo.get('Time 2', 'N/A')}</b> ({jogo.get('Horário', 'N/A')})\n"
            f"País: {jogo.get('País', 'N/A')}\n\n"
            f"🔥 <b>MÉDIA GERAL: {jogo.get('MÉDIA_PROB', 'N/A'):.0f}%</b> (CRITÉRIO OK)\n"
            f"✔️ <b>PARTIDAS: {jogo.get('Partidas', 'N/A'):.0f}</b> (CRITÉRIO OK)\n\n"
            f"📊 Probabilidades Detalhadas:\n"
            f"• Over 1.5: {jogo.get('Over15_MEDIA', 'N/A'):.0f}%\n" 
            f"• Over 2.5: {jogo.get('Over25_MEDIA', 'N/A'):.0f}%\n" 
            f"• Ambas Marcam: {jogo.get('Over_BOTH', 'N/A'):.0f}%\n" 
            f"Aproveite a oportunidade!"
        )
        
        for chat_id in usuarios:
            enviar_mensagem(chat_id, mensagem, token)