#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de Segmentação de Áudio usando Silero VAD
Implementação baseada no projeto original Katube com melhorias de modularização
Preparado para futura integração com Streamlit
"""

import os
import torch
import torchaudio
import time
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass


@dataclass
class SegmentationConfig:
    """
    Configurações para segmentação de áudio
    Todos os parâmetros baseados no projeto original com facilidade de modificação
    """
    # Duração dos segmentos (baseado no projeto original)
    min_duration_sec: float = 4.0      # Mínimo: 4 segundos - segmentos muito curtos são descartados
    max_duration_sec: float = 15.0     # Máximo: 15 segundos - segmentos longos são subdivididos
    
    # Parâmetros técnicos do VAD
    sampling_rate: int = 16000          # Taxa para análise VAD (Silero padrão)
    target_sampling_rate: int = 24000   # Taxa final dos segmentos salvos
    window_size_seconds: float = 0.15   # Janela de análise: 150ms (projeto original)
    
    # Controle de qualidade
    min_silence_for_split: float = 0.3  # Mínimo 300ms de silêncio para considerar divisão
    voice_threshold: float = 0.5        # Limiar de confiança do VAD (0.0-1.0)


class AudioSegmenter:
    """
    Segmentador de áudio usando Silero VAD
    Implementa cache do modelo para reutilização eficiente
    """
    
    def __init__(self):
        """Inicializa o segmentador sem carregar modelo (lazy loading)"""
        self.vad_model = None
        self.vad_utils = None
        self.vad_iterator = None
        self.model_loaded = False
        self.load_time = 0
        
    def load_vad_model(self) -> bool:
        """
        Carrega o modelo Silero VAD uma única vez para reutilização
        Baseado exatamente no projeto original
        
        Returns:
            bool: True se carregou com sucesso
        """
        if self.model_loaded:
            print("Modelo VAD já carregado, reutilizando...")
            return True
            
        try:
            print("Carregando modelo Silero VAD v4.0...")
            start_time = time.time()
            
            # Carrega modelo exatamente como no projeto original
            self.vad_model, self.vad_utils = torch.hub.load(
                repo_or_dir='snakers4/silero-vad:v4.0', 
                model='silero_vad', 
                trust_repo=True
            )
            
            # Extrai utilitários (mesmo padrão do original)
            (self.get_speech_timestamps, 
             self.save_audio, 
             self.read_audio, 
             self.VADIterator, 
             self.collect_chunks) = self.vad_utils
             
            # Cria iterator para análise de janelas
            self.vad_iterator = self.VADIterator(self.vad_model)
            
            self.load_time = time.time() - start_time
            self.model_loaded = True
            
            print(f"Modelo VAD carregado em {self.load_time:.2f}s")
            return True
            
        except Exception as e:
            print(f"Erro ao carregar modelo VAD: {e}")
            return False
    
    def _already_segmented(self, audio_path: str) -> bool:
        """
        Verifica se áudio já foi segmentado
        Implementa opção 3 - verificação configurável
        
        Args:
            audio_path: Caminho do arquivo de áudio original
            
        Returns:
            bool: True se já foi segmentado
        """
        segments_dir = self._get_output_directory(audio_path)
        
        # Verifica se pasta segments existe e tem arquivos .wav
        if segments_dir.exists():
            wav_files = list(segments_dir.glob("*.wav"))
            if wav_files:
                print(f"Segmentação já existe: {len(wav_files)} arquivos em {segments_dir}")
                return True
        
        return False
    
    def find_natural_pauses(self, wav_data: torch.Tensor, start_sample: int, 
                           end_sample: int, config: SegmentationConfig) -> List[int]:
        """
        Encontra pausas naturais dentro de um segmento longo
        Implementação baseada no projeto original
        
        Args:
            wav_data: Dados de áudio como tensor
            start_sample: Amostra inicial do segmento
            end_sample: Amostra final do segmento
            config: Configurações de segmentação
            
        Returns:
            List[int]: Posições das pausas encontradas (em amostras)
        """
        pauses = []
        window_samples = int(config.sampling_rate * config.window_size_seconds)
        
        # Analisa em janelas pequenas procurando por silêncio
        for i in range(start_sample, end_sample, window_samples):
            chunk_end = min(i + window_samples, end_sample)
            chunk = wav_data[i:chunk_end]
            
            if len(chunk) < window_samples // 2:  # Chunk muito pequeno
                break
                
            # Usa VAD iterator para detectar fala/silêncio
            speech_dict = self.vad_iterator(chunk, return_seconds=False)
            
            # Se não detectou fala, é uma pausa
            if not speech_dict:
                pauses.append(i)
        
        # Reset do iterator para próxima análise
        self.vad_iterator.reset_states()
        return pauses
    
    def segment_single_audio(self, audio_path: str, 
                           config: SegmentationConfig = None,
                           overwrite: bool = False) -> Dict:
        """
        Segmenta um único arquivo de áudio
        Método principal da classe
        
        Args:
            audio_path: Caminho para arquivo de áudio
            config: Configurações (usa padrão se None)
            overwrite: Se True, re-segmenta mesmo se já existir
            
        Returns:
            Dict: Resultado da segmentação com estatísticas
        """
        if config is None:
            config = SegmentationConfig()
        
        # Verifica se já foi segmentado (opção 3 - configurável)
        if not overwrite and self._already_segmented(audio_path):
            return {
                "success": True,
                "skipped": True,
                "reason": "Já foi segmentado (use overwrite=True para forçar)",
                "audio_file": audio_path
            }
            
        # Garante que modelo está carregado
        if not self.load_vad_model():
            return {"success": False, "error": "Falha ao carregar modelo VAD"}
            
        try:
            print(f"Processando: {os.path.basename(audio_path)}")
            
            # Carrega áudio na taxa do VAD
            wav_data = self.read_audio(audio_path, sampling_rate=config.sampling_rate)
            
            # Detecta segmentos de fala
            speech_timestamps = self.get_speech_timestamps(
                wav_data, 
                self.vad_model, 
                sampling_rate=config.sampling_rate
            )
            
            print(f"Detectados {len(speech_timestamps)} segmentos de fala brutos")
            
            # Processa segmentos aplicando regras de duração
            processed_segments = self._process_speech_segments(
                wav_data, speech_timestamps, config
            )
            
            print(f"Segmentos finais após processamento: {len(processed_segments)}")
            
            # Salva segmentos no formato do projeto original
            saved_files = self._save_segments_to_disk(
                audio_path, wav_data, processed_segments, config
            )
            
            return {
                "success": True,
                "skipped": False,
                "audio_file": audio_path,
                "raw_segments_detected": len(speech_timestamps),
                "final_segments_count": len(processed_segments),
                "files_saved": saved_files,
                "output_directory": str(self._get_output_directory(audio_path))
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Erro na segmentação: {e}",
                "audio_file": audio_path
            }
    
    def _process_speech_segments(self, wav_data: torch.Tensor, 
                               speech_timestamps: List[Dict], 
                               config: SegmentationConfig) -> List[Dict]:
        """
        Processa segmentos brutos aplicando regras de duração
        Lógica baseada no projeto original
        
        Args:
            wav_data: Dados de áudio
            speech_timestamps: Segmentos detectados pelo VAD
            config: Configurações
            
        Returns:
            List[Dict]: Segmentos processados com start/end
        """
        processed = []
        min_samples = int(config.min_duration_sec * config.sampling_rate)
        max_samples = int(config.max_duration_sec * config.sampling_rate)
        
        for segment in speech_timestamps:
            start_sample = segment['start']
            end_sample = segment['end']
            duration_samples = end_sample - start_sample
            
            # Descarta segmentos muito curtos
            if duration_samples < min_samples:
                continue
                
            # Segmento dentro do limite máximo - aceita direto
            elif duration_samples <= max_samples:
                processed.append({
                    'start': start_sample,
                    'end': end_sample
                })
                
            # Segmento muito longo - tenta subdividir
            else:
                pauses = self.find_natural_pauses(
                    wav_data, start_sample, end_sample, config
                )
                
                if pauses:
                    # Subdivide nas pausas encontradas
                    segment_start = start_sample
                    
                    for pause_pos in pauses:
                        segment_duration = pause_pos - segment_start
                        
                        # Adiciona se estiver no range válido
                        if min_samples <= segment_duration <= max_samples:
                            processed.append({
                                'start': segment_start,
                                'end': pause_pos
                            })
                            segment_start = pause_pos
                    
                    # Adiciona último segmento se válido
                    final_duration = end_sample - segment_start
                    if final_duration >= min_samples:
                        processed.append({
                            'start': segment_start,
                            'end': end_sample
                        })
                else:
                    # Sem pausas naturais - divide forçadamente
                    current_pos = start_sample
                    while current_pos < end_sample:
                        chunk_end = min(current_pos + max_samples, end_sample)
                        
                        if (chunk_end - current_pos) >= min_samples:
                            processed.append({
                                'start': current_pos,
                                'end': chunk_end
                            })
                        
                        current_pos = chunk_end
        
        return processed
    
    def _save_segments_to_disk(self, original_audio_path: str, 
                             wav_data: torch.Tensor, 
                             segments: List[Dict], 
                             config: SegmentationConfig) -> List[str]:
        """
        Salva segmentos no disco seguindo estrutura do projeto original
        
        Args:
            original_audio_path: Caminho do áudio original
            wav_data: Dados de áudio carregados
            segments: Lista de segmentos processados
            config: Configurações
            
        Returns:
            List[str]: Lista de arquivos salvos
        """
        # Cria estrutura de diretórios (mesmo padrão do original)
        output_dir = self._get_output_directory(original_audio_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Carrega áudio na taxa de amostragem final para salvar
        wav_final = self.read_audio(original_audio_path, sampling_rate=config.target_sampling_rate)
        
        # Fator de conversão entre taxas de amostragem
        rate_factor = config.target_sampling_rate / config.sampling_rate
        
        saved_files = []
        video_id = self._extract_video_id(original_audio_path)
        
        print(f"Salvando {len(segments)} segmentos...")
        
        for i, segment in enumerate(segments):
            # Converte posições para taxa final
            start_final = int(segment['start'] * rate_factor)
            end_final = int(segment['end'] * rate_factor)
            
            # Extrai segmento
            segment_audio = wav_final[start_final:end_final]
            
            # Nome do arquivo (padrão do projeto original)
            filename = f"{video_id}_{i}.wav"
            filepath = output_dir / filename
            
            # Salva segmento
            self.save_audio(str(filepath), segment_audio, sampling_rate=config.target_sampling_rate)
            saved_files.append(str(filepath))
            
            # Log de progresso
            duration = (end_final - start_final) / config.target_sampling_rate
            print(f"  -> {filename}: {duration:.1f}s")
        
        return saved_files
    
    def _get_output_directory(self, audio_path: str) -> Path:
        """
        Determina diretório de saída baseado na estrutura dinâmica do projeto
        Funciona com IDs dinâmicos de playlist/canal/vídeo
        
        Args:
            audio_path: Caminho do arquivo de áudio
            
        Returns:
            Path: Diretório onde salvar os segmentos
        """
        audio_file = Path(audio_path)
        
        # Se está na estrutura downloads/tipo_id/video_id/
        if "downloads" in audio_file.parts:
            # O diretório pai do áudio é o diretório do vídeo
            video_dir = audio_file.parent
            return video_dir / "segments"
        else:
            # Fallback: cria pasta segments ao lado do áudio
            return audio_file.parent / "segments"
    
    def _extract_video_id(self, audio_path: str) -> str:
        """
        Extrai video_id do caminho do arquivo
        Funciona com nomes dinâmicos do YouTube
        
        Args:
            audio_path: Caminho do arquivo de áudio
            
        Returns:
            str: ID do vídeo ou nome base do arquivo
        """
        filename = Path(audio_path).stem
        
        # Remove extensões múltiplas se houver (.mp3, .wav, etc)
        while '.' in filename:
            filename = Path(filename).stem
            
        return filename
    
    def segment_batch(self, audio_files: List[str], 
                     config: SegmentationConfig = None,
                     overwrite: bool = False) -> Dict:
        """
        Segmenta múltiplos arquivos em lote
        Reutiliza modelo carregado para eficiência
        
        Args:
            audio_files: Lista de caminhos para arquivos de áudio
            config: Configurações (usa padrão se None)
            overwrite: Se True, re-segmenta mesmo se já existir
            
        Returns:
            Dict: Relatório consolidado do processamento em lote
        """
        if config is None:
            config = SegmentationConfig()
            
        print(f"Iniciando processamento em lote: {len(audio_files)} arquivos")
        print(f"Configuração overwrite: {overwrite}")
        
        results = []
        total_segments = 0
        successful = 0
        failed = 0
        skipped = 0
        
        for i, audio_file in enumerate(audio_files, 1):
            print(f"\n[{i}/{len(audio_files)}] Processando: {os.path.basename(audio_file)}")
            
            result = self.segment_single_audio(audio_file, config, overwrite)
            results.append(result)
            
            if result['success']:
                if result.get('skipped', False):
                    skipped += 1
                    print(f"Pulado: {result['reason']}")
                else:
                    successful += 1
                    total_segments += result['final_segments_count']
                    print(f"Sucesso: {result['final_segments_count']} segmentos")
            else:
                failed += 1
                print(f"Falha: {result['error']}")
        
        return {
            "batch_completed": True,
            "total_files": len(audio_files),
            "successful": successful,
            "failed": failed,
            "skipped": skipped,
            "total_segments_created": total_segments,
            "individual_results": results,
            "config_used": config
        }


# ========================================
# FUNÇÕES DE CONVENIÊNCIA PARA USO EXTERNO
# ========================================

def quick_segment(audio_path: str, min_duration: float = 4.0, 
                 max_duration: float = 15.0, overwrite: bool = False) -> Dict:
    """
    Função de conveniência para segmentação rápida
    Ideal para testes e uso simples
    
    Args:
        audio_path: Caminho do arquivo de áudio
        min_duration: Duração mínima em segundos
        max_duration: Duração máxima em segundos
        overwrite: Se True, re-segmenta mesmo se já existir
        
    Returns:
        Dict: Resultado da segmentação
    """
    config = SegmentationConfig(
        min_duration_sec=min_duration,
        max_duration_sec=max_duration
    )
    
    segmenter = AudioSegmenter()
    return segmenter.segment_single_audio(audio_path, config, overwrite)


def segment_from_downloads_folder(downloads_path: str = "downloads", 
                                overwrite: bool = False) -> Dict:
    """
    Segmenta todos os áudios encontrados na pasta downloads
    Funciona com estrutura dinâmica de IDs do YouTube
    
    Args:
        downloads_path: Caminho da pasta downloads
        overwrite: Se True, re-segmenta arquivos já processados
        
    Returns:
        Dict: Relatório do processamento em lote
    """
    downloads_dir = Path(downloads_path)
    
    if not downloads_dir.exists():
        return {
            "batch_completed": False,
            "error": f"Pasta downloads não encontrada: {downloads_path}"
        }
    
    # Busca recursiva por todos os arquivos .mp3
    audio_files = list(downloads_dir.rglob("*.mp3"))
    audio_paths = [str(f) for f in audio_files]
    
    if not audio_paths:
        return {
            "batch_completed": False,
            "error": f"Nenhum arquivo .mp3 encontrado em {downloads_path}"
        }
    
    print(f"Encontrados {len(audio_paths)} arquivos de áudio")
    
    segmenter = AudioSegmenter()
    return segmenter.segment_batch(audio_paths, overwrite=overwrite)


# ========================================
# EXECUÇÃO STANDALONE PARA TESTES
# ========================================

def _find_project_root() -> Optional[Path]:
    """
    Encontra a raiz do projeto procurando pela pasta downloads
    Busca nos diretórios pai até encontrar
    
    Returns:
        Path: Caminho da raiz do projeto ou None se não encontrar
    """
    current = Path.cwd()
    
    # Busca até 4 níveis acima (segurança)
    for _ in range(4):
        downloads_path = current / "downloads"
        if downloads_path.exists() and downloads_path.is_dir():
            return current
        current = current.parent
        
        # Se chegou na raiz do sistema, para
        if current == current.parent:
            break
    
    return None


def main():
    """
    Função principal para execução standalone
    Processa áudios na pasta downloads se existir
    """
    print("KATUBE AUDIO SEGMENTER - Usando Silero VAD")
    print("=" * 50)
    
    # Encontra raiz do projeto automaticamente
    project_root = _find_project_root()
    
    if project_root:
        downloads_path = project_root / "downloads"
        print(f"Pasta downloads encontrada em: {downloads_path}")
        print("Processando todos os áudios...")
        
        # Por padrão não sobrescreve (overwrite=False)
        # Para forçar re-segmentação, mude para overwrite=True
        result = segment_from_downloads_folder(str(downloads_path), overwrite=False)
        
        if result.get('batch_completed'):
            print(f"\nProcessamento concluído!")
            print(f"Arquivos processados: {result['successful']}")
            print(f"Arquivos pulados: {result['skipped']}")
            print(f"Falhas: {result['failed']}")
            print(f"Total de segmentos criados: {result['total_segments_created']}")
        else:
            print(f"\nErro: {result.get('error', 'Erro desconhecido')}")
    else:
        print("Pasta 'downloads' não encontrada no projeto")
        print("Execute primeiro o módulo de download ou verifique a estrutura do projeto")


if __name__ == "__main__":
    main()