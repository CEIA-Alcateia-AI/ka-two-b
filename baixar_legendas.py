import os
from urllib.parse import urlparse, parse_qs
# A importação agora é um pouco diferente, pegando a exceção específica que precisamos
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled

def extrair_id_video(url_ou_id):
    """
    Extrai o ID do vídeo de uma URL do YouTube ou retorna o próprio ID se já for um.
    """
    if 'youtube.com' in url_ou_id or 'youtu.be' in url_ou_id:
        parsed_url = urlparse(url_ou_id)
        video_id = parse_qs(parsed_url.query).get('v')
        if video_id:
            return video_id[0]
        if parsed_url.hostname == 'youtu.be':
            return parsed_url.path[1:]
    return url_ou_id

def baixar_legenda_compativel(video_id):
    """
    Versão compatível com bibliotecas antigas.
    Baixa a legenda em português, priorizando 'pt-BR' e depois 'pt'.
    Não diferencia entre manual e automática.
    """
    print(f"\n--- Processando vídeo: {video_id} ---")
    
    # A biblioteca antiga lida com a prioridade diretamente na chamada
    prioridade_idiomas = ['pt-BR', 'pt']
    
    try:
        # Usamos o método .get_transcript() que é mais antigo e deve existir na sua versão
        print(f"  -> Tentando buscar legendas em {prioridade_idiomas}...")
        conteudo_legenda = YouTubeTranscriptApi.get_transcript(video_id, languages=prioridade_idiomas)
        
        # Se chegou até aqui, a legenda foi encontrada e baixada
        print(f"  -> Legenda encontrada! Formatando e salvando...")
        
        texto_formatado = "\n".join([item['text'] for item in conteudo_legenda])
        
        # Nome de arquivo simplificado, pois não sabemos o tipo
        nome_arquivo = f"{video_id}_legenda_pt.txt"
        
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            f.write(texto_formatado)
            
        print(f"  [SUCESSO] Legenda salva como: '{nome_arquivo}'")

    except NoTranscriptFound:
        print("  [AVISO] Nenhuma legenda em Português (pt-BR ou pt) foi encontrada para este vídeo.")
    except TranscriptsDisabled:
        print("  [ERRO] As legendas estão desativadas para este vídeo.")
    except Exception as e:
        print(f"  [ERRO] Ocorreu um erro inesperado: {e}")

# --- PONTO DE PARTIDA DO SCRIPT ---
if __name__ == "__main__":
    
    # ====================================================================
    # AQUI: Edite esta lista com os URLs ou IDs dos vídeos do YouTube
    # ====================================================================
    videos_para_baixar = [
        "https://www.youtube.com/watch?v=w5PaCvHiV6w&list=PLD6DVnxUXB2yi_HTPcjZdxA0Kna6K8-Ng&index=3",
        # "MupqMRNds-E",
        # "https://youtu.be/3a_w-8K1a3o",
        # "ID_DE_VIDEO_QUE_NAO_EXISTE",
        # "https://www.youtube.com/watch?v=QdKjUn_Vz6s"
    ]
    # ====================================================================

    print("Iniciando o download de legendas (modo de compatibilidade)...")
    
    for video in videos_para_baixar:
        video_id = extrair_id_video(video)
        if video_id:
            baixar_legenda_compativel(video_id)
        else:
            print(f"\n--- [ERRO] Não foi possível extrair um ID válido de: '{video}' ---")
            
    print("\nProcesso finalizado.")