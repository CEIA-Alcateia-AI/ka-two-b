"""
Módulo de transcrição usando modelo freds0/distil-whisper-large-v3-ptbr
Baseado na documentação oficial do Hugging Face Transformers
"""
import time
import torch
from transformers import pipeline

class Freds0Transcriber:
    """
    Classe para transcrição de áudio usando modelo freds0 Whisper em português brasileiro
    
    Attributes:
        model_name (str): Nome do modelo no Hugging Face Hub
        pipe (Pipeline): Pipeline de transcrição do transformers
        device (str): Dispositivo usado (CPU ou GPU)
    """
    
    def __init__(self):
        """Inicializa o transcriber com configurações padrão"""
        self.model_name = "freds0/distil-whisper-large-v3-ptbr"
        self.pipe = None
        self.device = 0 if torch.cuda.is_available() else -1  # 0=GPU, -1=CPU
        self.load_time = 0  # Tempo gasto para carregar modelo
        
    def load_model(self):
        """
        Carrega o modelo de transcrição na memória
        
        Returns:
            bool: True se carregou com sucesso, False caso contrário
        """
        try:
            print(f"Carregando modelo: {self.model_name}")
            start_time = time.time()
            
            # Cria pipeline de reconhecimento de fala automático
            self.pipe = pipeline(
                "automatic-speech-recognition",  # Tipo de pipeline
                model=self.model_name,           # Modelo específico
                device=self.device               # Dispositivo (GPU/CPU)
            )
            
            # Calcula tempo de carregamento
            self.load_time = time.time() - start_time
            print(f"Modelo carregado em {self.load_time:.2f} segundos")
            
            # Detecta dispositivo usado
            device_type = "GPU" if torch.cuda.is_available() else "CPU"
            print(f"Usando: {device_type}")
            
            return True
            
        except Exception as e:
            print(f"Erro ao carregar modelo: {e}")
            return False
            
    def transcribe(self, audio_path):
        """
        Transcreve um arquivo de áudio para texto
        
        Args:
            audio_path (str): Caminho para o arquivo de áudio
            
        Returns:
            dict: Dicionário com resultado da transcrição
                - text (str): Texto transcrito
                - model (str): Nome do modelo usado
                - transcription_time (float): Tempo gasto na transcrição
                - audio_file (str): Caminho do arquivo processado
        """
        # Verifica se modelo está carregado
        if not self.pipe:
            print("Modelo não carregado. Carregando agora...")
            if not self.load_model():
                return {"error": "Falha ao carregar modelo"}
        
        try:
            print(f"Transcrevendo arquivo: {audio_path}")
            start_time = time.time()
            
            # Executa transcrição usando pipeline
            result = self.pipe(audio_path)
            
            # Calcula tempo de transcrição
            transcription_time = time.time() - start_time
            
            # Retorna resultado estruturado
            return {
                "text": result["text"],                    # Texto transcrito
                "model": self.model_name,                  # Modelo usado
                "transcription_time": transcription_time,  # Tempo gasto
                "audio_file": audio_path                   # Arquivo processado
            }
            
        except Exception as e:
            return {"error": f"Erro na transcrição: {e}"}