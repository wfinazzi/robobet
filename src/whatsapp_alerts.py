import pywhatkit as kit
from datetime import datetime, timedelta

def enviar_alerta_whatsapp(numero, mensagem):
    """
    Envia mensagem via WhatsApp usando o pywhatkit.
    O envio é agendado 1 minuto à frente do horário atual.
    """
    agora = datetime.now()
    hora_envio = agora.hour
    minuto_envio = (agora + timedelta(minutes=1)).minute

    try:
        print(f"Enviando mensagem para {numero} às {hora_envio}:{minuto_envio:02d}...")
        kit.sendwhatmsg(numero, mensagem, hora_envio, minuto_envio)
        print("Mensagem enviada com sucesso!")
    except Exception as e:
        print("Erro ao enviar mensagem:", e)


def enviar_sugestoes_do_dia(jogos):
    """
    Recebe uma lista de sugestões e envia todas em uma única mensagem.
    """
    mensagem = "⚽ *Sugestões de Apostas do Dia* ⚽\n\n"
    for jogo in jogos:
        mensagem += f"🏟️ {jogo['mandante']} x {jogo['visitante']}\n"
        mensagem += f"📊 Tipo: {jogo['tipo']}\n"
        mensagem += f"💡 Sugestão: {jogo['sugestao']}\n"
        mensagem += "—" * 20 + "\n"

    enviar_alerta_whatsapp("+55XXXXXXXXXX", mensagem)  # <-- coloque seu número aqui


if __name__ == "__main__":
    # Exemplo de uso
    jogos_exemplo = [
        {"mandante": "Bahia", "visitante": "Grêmio", "tipo": "Over 1.5", "sugestao": "Mais de 1.5 gols"},
        {"mandante": "Palmeiras", "visitante": "Fluminense", "tipo": "Mandante Forte", "sugestao": "Vitória Palmeiras"},
    ]

    enviar_sugestoes_do_dia(jogos_exemplo)
