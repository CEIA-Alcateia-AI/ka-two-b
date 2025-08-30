"""
Módulo de transcrição usando lgris/wav2vec2-large-xlsr-open-brazilian-portuguese-v2
Modelo Wav2Vec2 treinado em múltiplos datasets brasileiros
"""
import time
import torch
import os
import json
from datetime import datetime
from transformers import Wav2Vec2ForCTC, Wav2Vec2Processor
import librosa

class LgrisWav2VecTranscriber:
    """
    Classe para transcrição usando modelo Wav2Vec2 otimizado para português brasileiro
    
    Attributes:
        model_name (str): Nome do modelo no Hugging Face Hub
        model: Modelo Wav2Vec2 carregado
        processor: Processador de áudio do modelo
        device (str): Dispositivo usado (cuda/cpu)
    """
    
    def __init__(self):
        """Inicializa transcriber Wav2Vec2 com configurações padrão"""
        self.model_name = "facebook/wav2vec2-large-xlsr-53-portuguese"
        self.model = None
        self.processor = None
        self.device = self._detect_device()
        self.load_time = 0
        
    def _detect_device(self):
        """Detecta automaticamente o melhor dispositivo disponível"""
        if torch.cuda.is_available():
            print(f"GPU detectada: {torch.cuda.get_device_name(0)}")
            return "cuda"
        else:
            print("Usando CPU")
            return "cpu"
            
    def load_model(self):
        """Carrega modelo Wav2Vec2 na memória"""
        try:
            print(f"Carregando modelo: {self.model_name}")
            start_time = time.time()
            
            # Carrega processador e modelo separadamente
            self.processor = Wav2Vec2Processor.from_pretrained(self.model_name)
            self.model = Wav2Vec2ForCTC.from_pretrained(self.model_name).to(self.device)
            
            self.load_time = time.time() - start_time
            print(f"Modelo carregado em {self.load_time:.2f} segundos")
            
            device_type = "GPU" if torch.cuda.is_available() else "CPU"
            print(f"Usando: {device_type}")
            
            return True
            
        except Exception as e:
            print(f"Erro ao carregar modelo: {e}")
            return False
            
    def transcribe(self, audio_path):
        """Transcreve arquivo de áudio para texto usando Wav2Vec2"""
        if not self.model:
            print("Modelo não carregado. Carregando agora...")
            if not self.load_model():
                return {"error": "Falha ao carregar modelo"}
        
        try:
            print(f"Transcrevendo arquivo: {audio_path}")
            start_time = time.time()
            
            # Carrega áudio com librosa (formato esperado pelo Wav2Vec2)
            audio, sample_rate = librosa.load(audio_path, sr=16000)
            
            # Processa áudio para formato de entrada do modelo
            inputs = self.processor(
                audio, 
                sampling_rate=16000, 
                return_tensors="pt",
                padding=True
            ).to(self.device)
            
            # Executa inferência
            with torch.no_grad():
                logits = self.model(
                    inputs.input_values,
                    attention_mask=inputs.attention_mask
                ).logits
                
            # Decodifica resultado
            predicted_ids = torch.argmax(logits, dim=-1)
            transcription = self.processor.batch_decode(predicted_ids)[0].lower()
            
            transcription_time = time.time() - start_time
            
            # Prepara dados estruturados
            transcription_data = {
                "text": transcription,
                "model": self.model_name,
                "transcription_time": transcription_time,
                "audio_file": audio_path,
                "timestamp": datetime.now().isoformat(),
                "sample_rate": sample_rate
            }
            
            # Salva resultados
            self._save_results(transcription_data, audio_path)
            return transcription_data
            
        except Exception as e:
            return {"error": f"Erro na transcrição: {e}"}

    def _save_results(self, data, audio_path):
        """Salva resultados do Wav2Vec2 em formato específico"""
        os.makedirs("results", exist_ok=True)
        
        base_name = os.path.splitext(os.path.basename(audio_path))[0]
        model_suffix = "lgris-wav2vec2"
        
        # Arquivos específicos do Wav2Vec2
        json_file = f"results/{base_name}_{model_suffix}.json"
        txt_file = f"results/{base_name}_{model_suffix}.txt"
        
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        with open(txt_file, "w", encoding="utf-8") as f:
            f.write(data["text"])
            
        # Adiciona ao CSV dataset
        csv_file = "results/transcriptions.csv"
        with open(csv_file, "a", encoding="utf-8") as f:
            f.write(f"{data['audio_file']}|{data['text']}\n")
            
        print(f"Lgris Wav2Vec2 - Dados salvos: {json_file}")