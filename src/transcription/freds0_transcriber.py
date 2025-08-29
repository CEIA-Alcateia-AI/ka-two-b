"""
Módulo de transcrição usando modelo freds0/distil-whisper-large-v3-ptbr
Baseado na documentação oficial do Hugging Face Transformers
"""
import json
import os
from datetime import datetime
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
        self.device = self._detect_device()  # Linha alterada
        self.load_time = 0

    def _detect_device(self):
        """Detecta automaticamente o melhor dispositivo disponível"""
        if torch.cuda.is_available():
            print(f"GPU detectada: {torch.cuda.get_device_name(0)}")
            return 0
        else:
            print("Usando CPU")
            return -1
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
        Transcreve um arquivo de áudio para texto e salva resultados automaticamente
        Cria 3 formatos: JSON (completo), CSV (dataset), TXT (texto puro)
        """
        # Verifica se modelo está carregado
        if not self.pipe:
            print("Modelo não carregado. Carregando agora...")
            if not self.load_model():
                return {"error": "Falha ao carregar modelo"}
        
        try:
            print(f"Transcrevendo arquivo: {audio_path}")
            start_time = time.time()
            
            # Executa transcrição com suporte a áudios longos
            result = self.pipe(
                audio_path,
                return_timestamps=True,
                chunk_length_s=30,
                stride_length_s=5
            )
            
            transcription_time = time.time() - start_time
            
            # Prepara dados estruturados
            transcription_data = {
                "text": result["text"],
                "model": self.model_name,
                "transcription_time": transcription_time,
                "audio_file": audio_path,
                "timestamp": datetime.now().isoformat(),
                "chunks": result.get("chunks", [])
            }
            
            # Salva resultados em múltiplos formatos
            self._save_results(transcription_data, audio_path)
            
            return transcription_data
            
        except Exception as e:
            return {"error": f"Erro na transcrição: {e}"}

    def _save_results(self, data, audio_path):
        """Salva resultados em 3 formatos diferentes"""
        
        # Cria pasta results se não existir
        os.makedirs("results", exist_ok=True)
        
        # Extrai nome base do arquivo
        base_name = os.path.splitext(os.path.basename(audio_path))[0]
        
        # 1. Salva JSON completo (dados estruturados)
        json_file = f"results/{base_name}.json"
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Dados completos salvos: {json_file}")
        
        # 2. Salva TXT (texto puro)
        txt_file = f"results/{base_name}.txt"
        with open(txt_file, "w", encoding="utf-8") as f:
            f.write(data["text"])
        print(f"Texto salvo: {txt_file}")
        
        # 3. Adiciona ao CSV do dataset (formato Katube)
        csv_file = "results/transcriptions.csv"
        with open(csv_file, "a", encoding="utf-8") as f:
            f.write(f"{data['audio_file']}|{data['text']}\n")
        print(f"Adicionado ao dataset: {csv_file}")