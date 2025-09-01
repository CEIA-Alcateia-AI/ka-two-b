"""
Teste de transcrição com arquivo de áudio real
"""
import sys
import os

# Adiciona pasta src/transcription ao caminho Python
current_dir = os.path.dirname(os.path.abspath(__file__))
transcription_path = os.path.join(current_dir, 'src', 'transcription')
sys.path.insert(0, transcription_path)

# Importa nossa classe
from freds0_transcriber import Freds0Transcriber

def test_audio_transcription():
    """Testa transcrição de arquivo de áudio real"""
    
    # Caminho para o arquivo de áudio
    audio_file = "src/download/downloads/playlist_PLd6DVnxUXB2xbV5opTB9mXRMxROfIDQl_/TRPBY_lxJfE/TRPBY_lxJfE.mp3"
    
    # Verifica se arquivo existe
    if not os.path.exists(audio_file):
        print(f"Arquivo não encontrado: {audio_file}")
        return False
    
    print("=== TESTE TRANSCRICAO DE AUDIO ===")
    print(f"Arquivo: {audio_file}")
    
    # Cria instância do transcriber
    transcriber = Freds0Transcriber()
    
    # Carrega modelo (se não estiver carregado)
    print("Carregando modelo...")
    if not transcriber.load_model():
        print("Falha ao carregar modelo")
        return False
    
    # Executa transcrição
    print("Iniciando transcrição...")
    result = transcriber.transcribe(audio_file)
    
    # Verifica se houve erro
    if "error" in result:
        print(f"Erro na transcrição: {result['error']}")
        return False
    
    # Exibe resultados
    print("\n=== RESULTADO ===")
    print(f"Texto transcrito: {result['text']}")
    print(f"Modelo usado: {result['model']}")
    print(f"Tempo de transcrição: {result['transcription_time']:.2f} segundos")
    
    return True

if __name__ == "__main__":
    # Executa teste
    success = test_audio_transcription()
    
    if success:
        print("\nTranscrição realizada com sucesso!")
    else:
        print("\nProblema na transcrição")