"""
Teste da classe Freds0Transcriber
"""
import sys
import os

# Adiciona pasta src/transcription ao caminho Python
current_dir = os.path.dirname(os.path.abspath(__file__))
transcription_path = os.path.join(current_dir, 'src', 'transcription')
sys.path.insert(0, transcription_path)

# Importa nossa classe
from freds0_transcriber import Freds0Transcriber

def test_class_loading():
    """Testa se a classe carrega corretamente"""
    print("=== TESTE CLASSE FREDS0 ===")
    
    # Cria instância da classe
    transcriber = Freds0Transcriber()
    print(f"Classe criada. Modelo: {transcriber.model_name}")
    
    # Testa carregamento do modelo
    success = transcriber.load_model()
    
    if success:
        print("Classe funciona corretamente!")
        return True
    else:
        print("Problema na classe!")
        return False

if __name__ == "__main__":
    # Executa teste
    result = test_class_loading()
    
    if result:
        print("\nProximo passo: testar transcricao com audio real")
    else:
        print("\nPrecisa corrigir a classe primeiro")