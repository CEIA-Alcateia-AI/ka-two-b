#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modulo de Segmentacao de Audio usando Silero VAD
Implementacao baseada no projeto original com melhorias de modularizacao
Integrado com configuracoes centralizadas do config master
"""

import os
import torch
import torchaudio
import time
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

# Importa configuracoes centralizadas do config master
try:
    import sys
    # Adiciona o diretorio src ao path para importar config
    src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)))
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    from config import default_config
    
    print(f"Configuracoes carregadas do config master para segmentacao")
    
    # Usa configuracoes do config master
    MIN_DURATION_SEC = default_config.SEGMENTATION['min_duration_sec']
    MAX_DURATION_SEC = default_config.SEGMENTATION['max_duration_sec']
    SAMPLING_RATE = default_config.SEGMENTATION['sampling_rate']
    TARGET_SAMPLING_RATE = default_config.SEGMENTATION['target_sampling_rate']
    WINDOW_SIZE_SECONDS = default_config.SEGMENTATION['window_size_seconds']
    MIN_SILENCE_FOR_SPLIT = default_config.SEGMENTATION['min_silence_for_split']
    VOICE_THRESHOLD = default_config.SEGMENTATION['voice_threshold']
    
except ImportError as e:
    print(f"Aviso: Nao foi possivel importar config master, usando valores padrao: {e}")
    # Fallback para configuracoes padrao se config.py nao estiver disponivel
    MIN_DURATION_SEC = 4.0
    MAX_DURATION_SEC = 15.0
    SAMPLING_RATE = 16000
    TARGET_SAMPLING_RATE = 24000
    WINDOW_SIZE_SECONDS = 0.15
    MIN_SILENCE_FOR_SPLIT = 0.3
    VOICE_THRESHOLD = 0.5


@dataclass
class SegmentationConfig:
    """
    Configuracoes para segmentacao de audio
    Valores obtidos do config master ou fallback para padroes
    """
    # Duracao dos segmentos (baseado no projeto original)
    min_duration_sec: float = MIN_DURATION_SEC      # Do config master
    max_duration_sec: float = MAX_DURATION_SEC       # Do config master
    
    # Parametros tecnicos do VAD
    sampling_rate: int = SAMPLING_RATE               # Do config master
    target_sampling_rate: int = TARGET_SAMPLING_RATE # Do config master
    window_size_seconds: float = WINDOW_SIZE_SECONDS  # Do config master
    
    # Controle de qualidade
    min_silence_for_split: float = MIN_SILENCE_FOR_SPLIT # Do config master
    voice_threshold: float = VOICE_THRESHOLD              # Do config master


class AudioSegmenter:
    """
    Segmentador de audio usando Silero VAD
    Implementa cache do modelo para reutilizacao eficiente
    CODIGO ORIGINAL PRESERVADO com integracao ao config master
    """
    
    def __init__(self):
        """Inicializa o segmentador sem carregar modelo (lazy loading)"""
        self.vad_model = None
        self.vad_utils = None
        self.vad_iterator = None
        self.model_loaded = False
        self.load_time = 0
        
        print(f"AudioSegmenter inicializado com configuracoes do config master:")
        print(f"  Min/Max duracao: {MIN_DURATION_SEC}s - {MAX_DURATION_SEC}s")
        print(f"  Taxa de amostragem: {SAMPLING_RATE}Hz -> {TARGET_SAMPLING_RATE}Hz")
        print(f"  Limiar de voz: {VOICE_THRESHOLD}")
        
    def load_vad_model(self) -> bool:
        """
        Carrega o modelo Silero VAD uma unica vez para reutilizacao
        Baseado exatamente no projeto original
        
        Returns:
            bool: True se carregou com sucesso
        """
        if self.model_loaded:
            print("Modelo VAD ja carregado, reutilizando...")
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
            
            # Extrai utilitarios (mesmo padrao do original)
            (self.get_speech_timestamps, 
             self.save_audio, 
             self.read_audio, 
             self.VADIterator, 
             self.collect_chunks) = self.vad_utils
             
            # Cria iterator para analise de janelas
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
        Verifica se audio ja foi segmentado
        Implementa opcao 3 - verificacao configuravel
        
        Args:
            audio_path: Caminho do arquivo de audio original
            
        Returns:
            bool: True se ja foi segmentado
        """
        segments_dir = self._get_output_directory(audio_path)
        
        # Verifica se pasta segments existe e tem arquivos .wav
        if segments_dir.exists():
            wav_files = list(segments_dir.glob("*.wav"))
            if wav_files:
                print(f"Segmentacao ja existe: {len(wav_files)} arquivos em {segments_dir}")
                return True
        
        return False
    
    def find_natural_pauses(self, wav_data: torch.Tensor, start_sample: int, 
                           end_sample: int, config: SegmentationConfig) -> List[int]:
        """
        Encontra pausas naturais dentro de um segmento longo
        Implementacao baseada no projeto original
        
        Args:
            wav_data: Dados de audio como tensor
            start_sample: Amostra inicial do segmento
            end_sample: Amostra final do segmento
            config: Configuracoes de segmentacao
            
        Returns:
            List[int]: Posicoes das pausas encontradas (em amostras)
        """
        pauses = []
        window_samples = int(config.sampling_rate * config.window_size_seconds)
        
        # Analisa em janelas pequenas procurando por silencio
        for i in range(start_sample, end_sample, window_samples):
            chunk_end = min(i + window_samples, end_sample)
            chunk = wav_data[i:chunk_end]
            
            if len(chunk) < window_samples // 2:  # Chunk muito pequeno
                break
                
            # Usa VAD iterator para detectar fala/silencio
            speech_dict = self.vad_iterator(chunk, return_seconds=False)
            
            # Se nao detectou fala, eh uma pausa
            if not speech_dict:
                pauses.append(i)
        
        # Reset do iterator para proxima analise
        self.vad_iterator.reset_states()
        return pauses
    
    def segment_single_audio(self, audio_path: str, 
                           config: SegmentationConfig = None,
                           overwrite: bool = False) -> Dict:
        """
        Segmenta um unico arquivo de audio
        Metodo principal da classe
        
        Args:
            audio_path: Caminho para arquivo de audio
            config: Configuracoes (usa padrao com config master se None)
            overwrite: Se True, re-segmenta mesmo se ja existir
            
        Returns:
            Dict: Resultado da segmentacao com estatisticas
        """
        if config is None:
            config = SegmentationConfig()  # Usa configuracoes do config master
        
        # Verifica se ja foi segmentado (opcao 3 - configuravel)
        if not overwrite and self._already_segmented(audio_path):
            return {
                "success": True,
                "skipped": True,
                "reason": "Ja foi segmentado (use overwrite=True para forcar)",
                "audio_file": audio_path
            }
            
        # Garante que modelo esta carregado
        if not self.load_vad_model():
            return {"success": False, "error": "Falha ao carregar modelo VAD"}
            
        try:
            print(f"Processando: {os.path.basename(audio_path)}")
            
            # Carrega audio na taxa do VAD
            wav_data = self.read_audio(audio_path, sampling_rate=config.sampling_rate)
            
            # Detecta segmentos de fala
            speech_timestamps = self.get_speech_timestamps(
                wav_data, 
                self.vad_model, 
                sampling_rate=config.sampling_rate
            )
            
            print(f"Detectados {len(speech_timestamps)} segmentos de fala brutos")
            
            # Processa segmentos aplicando regras de duracao
            processed_segments = self._process_speech_segments(
                wav_data, speech_timestamps, config
            )
            
            print(f"Segmentos finais apos processamento: {len(processed_segments)}")
            
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
                "error": f"Erro na segmentacao: {e}",
                "audio_file": audio_path
            }
    
    def _process_speech_segments(self, wav_data: torch.Tensor, 
                               speech_timestamps: List[Dict], 
                               config: SegmentationConfig) -> List[Dict]:
        """
        Processa segmentos brutos aplicando regras de duracao
        Logica baseada no projeto original
        
        Args:
            wav_data: Dados de audio
            speech_timestamps: Segmentos detectados pelo VAD
            config: Configuracoes
            
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
                
            # Segmento dentro do limite maximo - aceita direto
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
                        
                        # Adiciona se estiver no range valido
                        if min_samples <= segment_duration <= max_samples:
                            processed.append({
                                'start': segment_start,
                                'end': pause_pos
                            })
                            segment_start = pause_pos
                    
                    # Adiciona ultimo segmento se valido
                    final_duration = end_sample - segment_start
                    if final_duration >= min_samples:
                        processed.append({
                            'start': segment_start,
                            'end': end_sample
                        })
                else:
                    # Sem pausas naturais - divide forcadamente
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
            original_audio_path: Caminho do audio original
            wav_data: Dados de audio carregados
            segments: Lista de segmentos processados
            config: Configuracoes
            
        Returns:
            List[str]: Lista de arquivos salvos
        """
        # Cria estrutura de diretorios (mesmo padrao do original)
        output_dir = self._get_output_directory(original_audio_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Carrega audio na taxa de amostragem final para salvar
        wav_final = self.read_audio(original_audio_path, sampling_rate=config.target_sampling_rate)
        
        # Fator de conversao entre taxas de amostragem
        rate_factor = config.target_sampling_rate / config.sampling_rate
        
        saved_files = []
        video_id = self._extract_video_id(original_audio_path)
        
        print(f"Salvando {len(segments)} segmentos...")
        
        for i, segment in enumerate(segments):
            # Converte posicoes para taxa final
            start_final = int(segment['start'] * rate_factor)
            end_final = int(segment['end'] * rate_factor)
            
            # Extrai segmento
            segment_audio = wav_final[start_final:end_final]
            
            # Nome do arquivo (padrao do projeto original)
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
        Determina diretorio de saida baseado na estrutura dinamica do projeto
        Funciona com IDs dinamicos de playlist/canal/video
        
        Args:
            audio_path: Caminho do arquivo de audio
            
        Returns:
            Path: Diretorio onde salvar os segmentos
        """
        audio_file = Path(audio_path)
        
        # Se esta na estrutura downloads/tipo_id/video_id/
        if "downloads" in audio_file.parts:
            # O diretorio pai do audio eh o diretorio do video
            video_dir = audio_file.parent
            return video_dir / "segments"
        else:
            # Fallback: cria pasta segments ao lado do audio
            return audio_file.parent / "segments"
    
    def _extract_video_id(self, audio_path: str) -> str:
        """
        Extrai video_id do caminho do arquivo
        Funciona com nomes dinamicos do YouTube
        
        Args:
            audio_path: Caminho do arquivo de audio
            
        Returns:
            str: ID do video ou nome base do arquivo
        """
        filename = Path(audio_path).stem
        
        # Remove extensoes multiplas se houver (.mp3, .wav, etc)
        while '.' in filename:
            filename = Path(filename).stem
            
        return filename
    
    def segment_batch(self, audio_files: List[str], 
                     overwrite: bool = False) -> Dict:
        """
        Segmenta multiplos arquivos em lote
        Reutiliza modelo carregado para eficiencia
        
        Args:
            audio_files: Lista de caminhos para arquivos de audio
            overwrite: Se True, re-segmenta mesmo se ja existir
            
        Returns:
            Dict: Relatorio consolidado do processamento em lote
        """
        print(f"Iniciando processamento em lote: {len(audio_files)} arquivos")
        print(f"Configuracao overwrite: {overwrite}")
        print(f"Usando parametros do config master:")
        print(f"  Duracao: {MIN_DURATION_SEC}s - {MAX_DURATION_SEC}s")
        print(f"  Limiar VAD: {VOICE_THRESHOLD}")
        
        results = []
        total_segments = 0
        successful = 0
        failed = 0
        skipped = 0
        
        for i, audio_file in enumerate(audio_files, 1):
            print(f"\n[{i}/{len(audio_files)}] Processando: {os.path.basename(audio_file)}")
            
            result = self.segment_single_audio(audio_file, overwrite=overwrite)
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
            "config_used": SegmentationConfig()
        }


# ========================================
# FUNCOES DE CONVENIENCIA PARA USO EXTERNO
# ========================================

def quick_segment(audio_path: str, min_duration: float = MIN_DURATION_SEC, 
                 max_duration: float = MAX_DURATION_SEC, overwrite: bool = False) -> Dict:
    """
    Funcao de conveniencia para segmentacao rapida
    Ideal para testes e uso simples
    
    Args:
        audio_path: Caminho do arquivo de audio
        min_duration: Duracao minima em segundos (usa config master por padrao)
        max_duration: Duracao maxima em segundos (usa config master por padrao)
        overwrite: Se True, re-segmenta mesmo se ja existir
        
    Returns:
        Dict: Resultado da segmentacao
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
    Segmenta todos os audios encontrados na pasta downloads
    Funciona com estrutura dinamica de IDs do YouTube
    
    Args:
        downloads_path: Caminho da pasta downloads
        overwrite: Se True, re-segmenta arquivos ja processados
        
    Returns:
        Dict: Relatorio do processamento em lote
    """
    downloads_dir = Path(downloads_path)
    
    if not downloads_dir.exists():
        return {
            "batch_completed": False,
            "error": f"Pasta downloads nao encontrada: {downloads_path}"
        }
    
    # Busca recursiva por todos os arquivos .mp3
    audio_files = list(downloads_dir.rglob("*.mp3"))
    audio_paths = [str(f) for f in audio_files]
    
    if not audio_paths:
        return {
            "batch_completed": False,
            "error": f"Nenhum arquivo .mp3 encontrado em {downloads_path}"
        }
    
    print(f"Encontrados {len(audio_paths)} arquivos de audio")
    
    segmenter = AudioSegmenter()
    return segmenter.segment_batch(audio_paths, overwrite=overwrite)


# ========================================
# EXECUCAO STANDALONE PARA TESTES
# ========================================

def _find_project_root() -> Optional[Path]:
    """
    Encontra a raiz do projeto procurando pela pasta downloads
    Busca nos diretorios pai ate encontrar
    
    Returns:
        Path: Caminho da raiz do projeto ou None se nao encontrar
    """
    current = Path.cwd()
    
    # Busca ate 4 niveis acima (seguranca)
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
    Funcao principal para execucao standalone
    Processa audios na pasta downloads se existir
    """
    print("KATUBE AUDIO SEGMENTER - Integrado com Config Master")
    print("=" * 60)
    
    # Encontra raiz do projeto automaticamente
    project_root = _find_project_root()
    
    if project_root:
        downloads_path = project_root / "downloads"
        print(f"Pasta downloads encontrada em: {downloads_path}")
        print("Processando todos os audios...")
        
        # Por padrao nao sobrescreve (overwrite=False)
        # Para forcar re-segmentacao, mude para overwrite=True
        result = segment_from_downloads_folder(str(downloads_path), overwrite=False)
        
        if result.get('batch_completed'):
            print(f"\nProcessamento concluido!")
            print(f"Arquivos processados: {result['successful']}")
            print(f"Arquivos pulados: {result['skipped']}")
            print(f"Falhas: {result['failed']}")
            print(f"Total de segmentos criados: {result['total_segments_created']}")
            
            if result.get('config_used'):
                config = result['config_used']
                print(f"\nConfiguracoes aplicadas do config master:")
                print(f"  Duracao min/max: {config.min_duration_sec}s - {config.max_duration_sec}s")
                print(f"  Taxa de amostragem: {config.sampling_rate}Hz -> {config.target_sampling_rate}Hz")
                print(f"  Limiar de voz: {config.voice_threshold}")
                
        else:
            print(f"\nErro: {result.get('error', 'Erro desconhecido')}")
    else:
        print("Pasta 'downloads' nao encontrada no projeto")
        print("Execute primeiro o modulo de download ou verifique a estrutura do projeto")
        print("\nEstrutura esperada:")
        print("  downloads/*/segments/*.wav")


if __name__ == "__main__":
    main()