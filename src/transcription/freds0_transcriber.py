#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modulo de Transcricao usando modelo freds0/distil-whisper-large-v3-ptbr
Implementacao modular para processamento batch de segmentos de audio
Preparado para integracao com validacao cruzada e interface Streamlit
"""

import os
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass
import torch
from transformers import pipeline


@dataclass
class Freds0TranscriptionConfig:
    """
    Configuracoes para transcricao com modelo freds0 Whisper
    Parametros otimizados para qualidade e compatibilidade
    """
    # Configuracoes do modelo
    model_name: str = "freds0/distil-whisper-large-v3-ptbr"
    
    # Configuracoes de processamento
    chunk_length_s: int = 30         # Segundos por chunk (otimo para Whisper)
    stride_length_s: int = 5         # Sobreposicao entre chunks
    return_timestamps: bool = True   # Timestamps internos do modelo
    
    # Configuracoes de qualidade
    min_audio_duration: float = 1.0  # Minimo em segundos
    max_audio_duration: float = 60.0 # Maximo em segundos
    
    # Configuracoes de output
    output_filename: str = "transcricoes_freds0.json"
    overwrite_existing: bool = False  # Nao sobrescreve por padrao


class Freds0Transcriber:
    """
    Transcritor usando modelo freds0 Whisper para portugues brasileiro
    Implementa processamento batch e interface padronizada
    """
    
    def __init__(self, config: Optional[Freds0TranscriptionConfig] = None):
        """
        Inicializa transcritor com configuracao personalizada
        
        Args:
            config: Configuracao customizada (usa padrao se None)
        """
        self.config = config or Freds0TranscriptionConfig()
        self.pipe = None
        self.device = self._detect_device()
        self.model_loaded = False
        self.load_time = 0
        
        # Estatisticas de processamento
        self.stats = {
            'total_files_processed': 0,
            'successful_transcriptions': 0,
            'failed_transcriptions': 0,
            'total_processing_time': 0,
            'average_processing_time': 0
        }
    
    def _detect_device(self) -> int:
        """
        Detecta automaticamente o melhor dispositivo disponivel
        Retorna formato esperado pelo pipeline transformers
        """
        if torch.cuda.is_available():
            device_name = torch.cuda.get_device_name(0)
            print(f"GPU detectada para freds0: {device_name}")
            return 0  # GPU
        else:
            print("Usando CPU para freds0 Whisper")
            return -1  # CPU
    
    def load_model(self) -> bool:
        """
        Carrega modelo freds0 Whisper uma unica vez para reutilizacao
        Implementa lazy loading para eficiencia de memoria
        
        Returns:
            bool: True se carregou com sucesso
        """
        if self.model_loaded:
            print("Modelo freds0 ja carregado, reutilizando...")
            return True
        
        try:
            print(f"Carregando modelo freds0: {self.config.model_name}")
            start_time = time.time()
            
            # Cria pipeline de reconhecimento de fala automatico
            self.pipe = pipeline(
                "automatic-speech-recognition",
                model=self.config.model_name,
                device=self.device,
                torch_dtype=torch.float16 if self.device == 0 else torch.float32
            )
            
            self.load_time = time.time() - start_time
            self.model_loaded = True
            
            device_type = "GPU" if self.device == 0 else "CPU"
            print(f"Modelo freds0 carregado em {self.load_time:.2f}s no {device_type}")
            return True
            
        except Exception as e:
            print(f"Erro ao carregar modelo freds0: {e}")
            return False
    
    def transcribe_single_audio(self, audio_path: str) -> Dict:
        """
        Transcreve um unico arquivo de audio
        Metodo principal para transcricao individual
        
        Args:
            audio_path: Caminho para arquivo de audio
            
        Returns:
            Dict: Resultado da transcricao com metadados
        """
        # Verifica se modelo esta carregado
        if not self.load_model():
            return {
                "success": False,
                "error": "Falha ao carregar modelo freds0",
                "audio_file": audio_path
            }
        
        try:
            print(f"Transcrevendo com freds0: {os.path.basename(audio_path)}")
            start_time = time.time()
            
            # Verifica se arquivo existe
            if not os.path.exists(audio_path):
                return {
                    "success": False,
                    "error": f"Arquivo nao encontrado: {audio_path}",
                    "audio_file": audio_path,
                    "timestamp": datetime.now().isoformat()
                }
            
            # Executa transcricao com suporte a audios longos
            result = self.pipe(
                audio_path,
                return_timestamps=self.config.return_timestamps,
                chunk_length_s=self.config.chunk_length_s,
                stride_length_s=self.config.stride_length_s
            )
            
            transcription_time = time.time() - start_time
            
            # Extrai texto e timestamps se disponivel
            transcription_text = result.get("text", "")
            chunks_data = result.get("chunks", [])
            
            # Estima duracao do audio baseado nos timestamps ou no processamento
            if chunks_data and len(chunks_data) > 0:
                last_chunk = chunks_data[-1]
                if 'timestamp' in last_chunk and last_chunk['timestamp']:
                    duration = last_chunk['timestamp'][1] if last_chunk['timestamp'][1] else 0
                else:
                    duration = 0
            else:
                duration = 0
            
            # Atualiza estatisticas
            self.stats['successful_transcriptions'] += 1
            self.stats['total_processing_time'] += transcription_time
            
            return {
                "success": True,
                "text": transcription_text,
                "model": self.config.model_name,
                "transcription_time": transcription_time,
                "audio_file": audio_path,
                "duration": duration,
                "chunks": chunks_data,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.stats['failed_transcriptions'] += 1
            return {
                "success": False,
                "error": f"Erro na transcricao freds0: {e}",
                "audio_file": audio_path,
                "timestamp": datetime.now().isoformat()
            }
    
    def find_all_segment_directories(self, downloads_base: str = "downloads") -> List[str]:
        """
        Encontra todos os diretorios segments/ na estrutura downloads/
        Funciona com estrutura dinamica de IDs do YouTube
        
        Args:
            downloads_base: Diretorio base dos downloads
            
        Returns:
            List[str]: Lista de caminhos para diretorios segments/
        """
        downloads_path = Path(downloads_base)
        
        if not downloads_path.exists():
            print(f"Diretorio downloads nao encontrado: {downloads_base}")
            return []
        
        # Busca recursiva por pastas 'segments'
        segment_dirs = []
        for segments_dir in downloads_path.rglob("segments"):
            if segments_dir.is_dir():
                # Verifica se tem arquivos .wav
                wav_files = list(segments_dir.glob("*.wav"))
                if wav_files:
                    segment_dirs.append(str(segments_dir))
                    print(f"Encontrado: {segments_dir} ({len(wav_files)} arquivos .wav)")
        
        print(f"Total de diretorios segments encontrados: {len(segment_dirs)}")
        return segment_dirs
    
    def transcribe_segments_directory(self, segments_dir: str, 
                                    overwrite: bool = None) -> Dict:
        """
        Transcreve todos os segmentos em um diretorio
        Salva resultado em JSON padronizado (formato compativel com lgris)
        
        Args:
            segments_dir: Diretorio com arquivos .wav
            overwrite: Sobrescrever arquivo existente (usa config se None)
            
        Returns:
            Dict: Relatorio da transcricao do diretorio
        """
        segments_path = Path(segments_dir)
        overwrite_files = overwrite if overwrite is not None else self.config.overwrite_existing
        
        # Arquivo de output no mesmo diretorio
        output_file = segments_path / self.config.output_filename
        
        # Verifica se ja foi processado
        if output_file.exists() and not overwrite_files:
            print(f"Transcricao freds0 ja existe: {output_file}")
            return {
                "directory": segments_dir,
                "status": "skipped",
                "reason": "Arquivo ja existe (use overwrite=True para forcar)",
                "output_file": str(output_file)
            }
        
        # Encontra arquivos WAV
        wav_files = sorted(segments_path.glob("*.wav"))
        
        if not wav_files:
            return {
                "directory": segments_dir,
                "status": "error",
                "error": "Nenhum arquivo .wav encontrado",
                "wav_count": 0
            }
        
        print(f"Transcrevendo {len(wav_files)} segmentos em: {segments_dir}")
        
        # Dados de output padronizados (formato compativel com lgris)
        transcription_data = {
            "metadata": {
                "model": self.config.model_name,
                "directory": segments_dir,
                "total_segments": len(wav_files),
                "processing_date": datetime.now().isoformat(),
                "config": {
                    "chunk_length_s": self.config.chunk_length_s,
                    "stride_length_s": self.config.stride_length_s,
                    "return_timestamps": self.config.return_timestamps
                }
            },
            "transcriptions": {},
            "stats": {
                "successful": 0,
                "failed": 0,
                "total_duration": 0,
                "total_processing_time": 0
            }
        }
        
        # Processa cada arquivo
        for wav_file in wav_files:
            result = self.transcribe_single_audio(str(wav_file))
            
            segment_key = wav_file.stem  # Nome sem extensao
            
            if result["success"]:
                transcription_data["transcriptions"][segment_key] = {
                    "text": result["text"],
                    "transcription_time": result["transcription_time"],
                    "duration": result.get("duration", 0),
                    "timestamp": result["timestamp"],
                    "chunks": result.get("chunks", [])
                }
                transcription_data["stats"]["successful"] += 1
                transcription_data["stats"]["total_duration"] += result.get("duration", 0)
                transcription_data["stats"]["total_processing_time"] += result["transcription_time"]
            else:
                transcription_data["transcriptions"][segment_key] = {
                    "error": result["error"],
                    "timestamp": result.get("timestamp", datetime.now().isoformat())
                }
                transcription_data["stats"]["failed"] += 1
        
        # Salva resultado em arquivo JSON
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(transcription_data, f, indent=2, ensure_ascii=False)
            
            print(f"Transcricoes salvas: {output_file}")
            print(f"Sucessos: {transcription_data['stats']['successful']}/{len(wav_files)}")
            
            return {
                "directory": segments_dir,
                "status": "completed",
                "output_file": str(output_file),
                "stats": transcription_data["stats"],
                "wav_files_processed": len(wav_files)
            }
            
        except Exception as e:
            return {
                "directory": segments_dir,
                "status": "error",
                "error": f"Erro ao salvar transcricoes: {e}",
                "wav_files_processed": len(wav_files)
            }
    
    def transcribe_all_segments_batch(self, downloads_base: str = "downloads",
                                    overwrite: bool = False) -> Dict:
        """
        Processa em lote todos os segmentos encontrados na estrutura downloads/
        Metodo principal para processamento completo
        
        Args:
            downloads_base: Diretorio base dos downloads
            overwrite: Sobrescrever transcricoes existentes
            
        Returns:
            Dict: Relatorio consolidado do processamento batch
        """
        print("Iniciando processamento batch freds0 Whisper")
        print(f"Diretorio base: {downloads_base}")
        print(f"Sobrescrever existentes: {overwrite}")
        
        batch_start_time = time.time()
        
        # Encontra todos os diretorios segments
        segment_directories = self.find_all_segment_directories(downloads_base)
        
        if not segment_directories:
            return {
                "status": "error",
                "error": "Nenhum diretorio segments/ encontrado",
                "downloads_base": downloads_base
            }
        
        # Processa cada diretorio
        results = []
        successful_dirs = 0
        failed_dirs = 0
        total_segments_processed = 0
        
        for segments_dir in segment_directories:
            print(f"\nProcessando diretorio: {segments_dir}")
            
            result = self.transcribe_segments_directory(segments_dir, overwrite)
            results.append(result)
            
            if result["status"] == "completed":
                successful_dirs += 1
                total_segments_processed += result["wav_files_processed"]
            elif result["status"] == "error":
                failed_dirs += 1
            # status "skipped" nao conta como falha
        
        batch_total_time = time.time() - batch_start_time
        
        # Atualiza estatisticas gerais
        self.stats['total_files_processed'] = total_segments_processed
        if self.stats['successful_transcriptions'] > 0:
            self.stats['average_processing_time'] = (
                self.stats['total_processing_time'] / self.stats['successful_transcriptions']
            )
        
        return {
            "status": "completed",
            "batch_summary": {
                "total_directories": len(segment_directories),
                "successful_directories": successful_dirs,
                "failed_directories": failed_dirs,
                "skipped_directories": len(segment_directories) - successful_dirs - failed_dirs,
                "total_segments_processed": total_segments_processed,
                "batch_processing_time": batch_total_time
            },
            "detailed_results": results,
            "model_stats": self.stats,
            "config_used": self.config
        }
    
    def get_transcription_status(self, downloads_base: str = "downloads") -> Dict:
        """
        Verifica status das transcricoes sem processar
        Util para interfaces e monitoramento
        
        Args:
            downloads_base: Diretorio base dos downloads
            
        Returns:
            Dict: Status atual das transcricoes
        """
        segment_directories = self.find_all_segment_directories(downloads_base)
        
        status_summary = {
            "total_directories": len(segment_directories),
            "transcribed": 0,
            "pending": 0,
            "directories_status": []
        }
        
        for segments_dir in segment_directories:
            segments_path = Path(segments_dir)
            output_file = segments_path / self.config.output_filename
            wav_count = len(list(segments_path.glob("*.wav")))
            
            dir_status = {
                "directory": segments_dir,
                "wav_files_count": wav_count,
                "transcription_exists": output_file.exists()
            }
            
            if output_file.exists():
                status_summary["transcribed"] += 1
                # Carrega metadata se disponivel
                try:
                    with open(output_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    dir_status["last_processed"] = data["metadata"]["processing_date"]
                    dir_status["successful_segments"] = data["stats"]["successful"]
                except:
                    dir_status["metadata_error"] = True
            else:
                status_summary["pending"] += 1
            
            status_summary["directories_status"].append(dir_status)
        
        return status_summary


# ========================================
# FUNCOES DE CONVENIENCIA PARA USO EXTERNO
# ========================================

def quick_transcribe_freds0(segments_dir: str, overwrite: bool = False) -> Dict:
    """
    Transcricao rapida de um diretorio especifico
    Funcao de conveniencia para uso simples
    
    Args:
        segments_dir: Diretorio com segmentos .wav
        overwrite: Sobrescrever se ja existe
        
    Returns:
        Dict: Resultado da transcricao
    """
    transcriber = Freds0Transcriber()
    return transcriber.transcribe_segments_directory(segments_dir, overwrite)


def batch_transcribe_all_freds0(downloads_path: str = "downloads", 
                               overwrite: bool = False) -> Dict:
    """
    Processamento batch de todos os segmentos
    Interface simplificada para execucao completa
    
    Args:
        downloads_path: Diretorio base downloads
        overwrite: Sobrescrever transcricoes existentes
        
    Returns:
        Dict: Relatorio consolidado
    """
    transcriber = Freds0Transcriber()
    return transcriber.transcribe_all_segments_batch(downloads_path, overwrite)


def check_freds0_transcription_status(downloads_path: str = "downloads") -> Dict:
    """
    Verifica status das transcricoes sem processar
    Util para monitoramento e interfaces
    
    Args:
        downloads_path: Diretorio base downloads
        
    Returns:
        Dict: Status atual detalhado
    """
    transcriber = Freds0Transcriber()
    return transcriber.get_transcription_status(downloads_path)


# ========================================
# EXECUCAO STANDALONE PARA TESTES
# ========================================

def _find_test_segments() -> Optional[str]:
    """
    Procura diretorios segments para teste
    Baseado na estrutura conhecida do projeto
    """
    test_paths = [
        "downloads",
        "../downloads", 
        "../../downloads"
    ]
    
    for base_path in test_paths:
        if Path(base_path).exists():
            transcriber = Freds0Transcriber()
            segment_dirs = transcriber.find_all_segment_directories(base_path)
            if segment_dirs:
                print(f"Encontrados dados de teste: {len(segment_dirs)} diretorios")
                return base_path
    
    return None


def main():
    """
    Funcao principal para execucao standalone
    Processa todos os segmentos encontrados na estrutura do projeto
    """
    print("KATUBE FREDS0 TRANSCRIBER - Whisper Portugues Brasileiro")
    print("=" * 65)
    
    # Procura dados de teste
    downloads_base = _find_test_segments()
    
    if downloads_base:
        # Verifica status atual
        print("Verificando status das transcricoes...")
        status = check_freds0_transcription_status(downloads_base)
        
        print(f"Diretorios encontrados: {status['total_directories']}")
        print(f"Ja transcritos: {status['transcribed']}")
        print(f"Pendentes: {status['pending']}")
        
        if status['pending'] > 0:
            print(f"\nProcessando {status['pending']} diretorios pendentes...")
            
            # Executa processamento batch (nao sobrescreve por padrao)
            result = batch_transcribe_all_freds0(downloads_base, overwrite=False)
            
            if result["status"] == "completed":
                summary = result["batch_summary"]
                print(f"\nProcessamento concluido!")
                print(f"Diretorios processados: {summary['successful_directories']}")
                print(f"Segmentos transcritos: {summary['total_segments_processed']}")
                print(f"Tempo total: {summary['batch_processing_time']:.2f}s")
                
                if summary['failed_directories'] > 0:
                    print(f"Falhas: {summary['failed_directories']} diretorios")
            else:
                print(f"Erro no processamento: {result.get('error', 'Erro desconhecido')}")
        else:
            print("Todas as transcricoes ja foram realizadas!")
            print("Use overwrite=True para reprocessar")
    else:
        print("Nenhum diretorio segments/ encontrado")
        print("Execute primeiro os modulos de download e segmentacao")
        print("\nEstrutura esperada:")
        print("  downloads/*/segments/*.wav")


if __name__ == "__main__":
    main()