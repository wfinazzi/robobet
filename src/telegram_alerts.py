# telegram_alerts.py
import requests
import datetime

# --- Função para enviar mensagem para um chat específico ---
def enviar_mensagem(chat_id, mensagem, token):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": mensagem,
        "parse_mode": "HTML"  # permite usar <b>, <i> etc
    }
    try:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            print(f"Mensagem enviada para {chat_id}")
        else:
            print(f"Erro {response.status_code} ao enviar para {chat_id}")
    except Exception as e:
        print(f"Erro ao enviar mensagem: {e}")

# --- Função para enviar alertas de jogos próximos ---
def enviar_alertas(df, token, usuarios, minutos_antes=30):
    """
    df: DataFrame com os jogos filtrados
    token: token do bot do Telegram
    usuarios: lista de chat_ids
    minutos_antes: quantos minutos antes do jogo enviar o alerta
    """
    agora = datetime.datetime.now()
    
    for _, jogo in df.iterrows():
        # Converte a coluna 'Horário' para datetime
        if isinstance(jogo['Horário'], str):
            jogo_hora = datetime.datetime.strptime(jogo['Horário'], "%H:%M").replace(
                year=agora.year, month=agora.month, day=agora.day
            )
        else:
            jogo_hora = datetime.datetime.combine(datetime.date.today(), jogo['Horário'])
        
        # Calcula diferença em minutos
        # diff = (jogo_hora - agora).total_seconds() / 60
        
        # Se o jogo estiver dentro do intervalo (meia hora antes)
        # if minutos_antes-5 <= diff <= minutos_antes+5:  # ±5 minutos de tolerância
        mensagem = (
			f"⚽ <b>{jogo['Time 1']}</b> x <b>{jogo['Time 2']}</b>\n"
			f"País: {jogo['País']}\n"
			f"Horário: {jogo['Horário']}\n"
			f"📊 Probabilidades:\n"
			f"• Over 1.5: {jogo['Prob_Over1.5']}%\n"
			f"• Over 2.5: {jogo['Prob_Over2.5']}%\n"
			f"• Ambas Marcam (BTTS): {jogo['Prob_BTTS']}%\n"
			f"Média Probabilidade: {jogo.get('MÉDIA_PROB', 'N/A')}%\n"
			f"Tipo de aposta sugerido: {jogo.get('Tipo_Aposta', 'Não definido')}"
		)
		
		# Envia para todos os usuários cadastrados
        for chat_id in usuarios:
            enviar_mensagem(chat_id, mensagem, token)
