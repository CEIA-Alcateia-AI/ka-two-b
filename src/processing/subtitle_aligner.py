#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de Alinhamento de Legendas com Segmentos de Áudio
Implementa alinhamento matemático baseado em timestamps do arquivo .srt
Preparado para futura integração com Streamlit
"""

import os
import re
import json
import time
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import librosa


@dataclass
class AlignmentConfig:
    """
    Configurações para alinhamento temporal
    Parâmetros ajustáveis baseados no projeto original
    """
    # Tolerância temporal para busca de correspondência
    time_tolerance_sec: float = 0.5     # Aceita diferença de até 500ms
    
    # Tratamento de segmentos sem correspondência
    allow_orphan_segments: bool = True  # Mantém segmentos sem texto correspondente
    min_text_length: int = 3           # Descarta textos muito curtos (ex: "ah", "hmm")
    
    # Configurações de output
    output_format: str = "json"        # Formato: json ou csv
    preserve_original_text: bool = True # Mantém texto original antes da normalização


class SubtitleParser:
    """
    Parser especializado para arquivos .srt
    Extrai timestamps e textos de forma robusta
    """
    
    @staticmethod
    def parse_srt_file(srt_path: str) -> List[Dict]:
        """
        Faz parse de arquivo .srt/.vtt extraindo todos os segmentos
        Suporta tanto formato SRT tradicional quanto WEBVTT do YouTube
        
        Args:
            srt_path: Caminho para arquivo .srt ou .vtt
            
        Returns:
            List[Dict]: Lista de segmentos com start_time, end_time, text
        """
        try:
            with open(srt_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            print(f"Erro ao ler arquivo de legenda: {e}")
            return []
        
        # Detecta se é formato WEBVTT ou SRT tradicional
        is_webvtt = content.startswith('WEBVTT')
        
        if is_webvtt:
            return SubtitleParser._parse_webvtt_content(content)
        else:
            return SubtitleParser._parse_srt_content(content)
    
    @staticmethod
    def _parse_webvtt_content(content: str) -> List[Dict]:
        """
        Parse específico para formato WEBVTT do YouTube
        
        Args:
            content: Conteúdo completo do arquivo WEBVTT
            
        Returns:
            List[Dict]: Segmentos extraídos
        """
        segments = []
        
        # Regex para capturar blocos WEBVTT
        # Formato: timestamp --> timestamp propriedades\ntexto (pode ter tags <c>)
        webvtt_pattern = r'(\d{2}:\d{2}:\d{2}\.\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}\.\d{3})[^\n]*\n(.*?)(?=\n\s*\d{2}:\d{2}:\d{2}\.\d{3}|\Z)'
        
        matches = re.findall(webvtt_pattern, content, re.DOTALL)
        
        for i, match in enumerate(matches):
            start_str, end_str, text_block = match
            
            try:
                # Converte timestamps WEBVTT para segundos
                start_seconds = SubtitleParser._webvtt_timestamp_to_seconds(start_str)
                end_seconds = SubtitleParser._webvtt_timestamp_to_seconds(end_str)
                
                # Limpa texto removendo tags HTML e formatação extra
                clean_text = SubtitleParser._clean_webvtt_text(text_block)
                
                if clean_text and len(clean_text.strip()) > 0:
                    segments.append({
                        'index': i + 1,
                        'start_time': start_seconds,
                        'end_time': end_seconds,
                        'text': clean_text,
                        'duration': end_seconds - start_seconds
                    })
                    
            except Exception as e:
                print(f"Erro ao processar segmento WEBVTT {i}: {e}")
                continue
        
        print(f"Parsed {len(segments)} segmentos do arquivo WEBVTT")
        return segments
    
    @staticmethod
    def _parse_srt_content(content: str) -> List[Dict]:
        """
        Parse para formato SRT tradicional (fallback)
        
        Args:
            content: Conteúdo do arquivo SRT
            
        Returns:
            List[Dict]: Segmentos extraídos
        """
        segments = []
        
        # Regex para SRT tradicional
        srt_pattern = r'(\d+)\s*\n(\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2},\d{3})\s*\n(.*?)(?=\n\s*\n|\n\s*\d+\s*\n|\Z)'
        
        matches = re.findall(srt_pattern, content, re.DOTALL)
        
        for match in matches:
            index, start_str, end_str, text = match
            
            try:
                start_seconds = SubtitleParser._timestamp_to_seconds(start_str)
                end_seconds = SubtitleParser._timestamp_to_seconds(end_str)
                
                clean_text = re.sub(r'\s+', ' ', text.strip())
                
                if clean_text:
                    segments.append({
                        'index': int(index),
                        'start_time': start_seconds,
                        'end_time': end_seconds,
                        'text': clean_text,
                        'duration': end_seconds - start_seconds
                    })
                    
            except Exception as e:
                print(f"Erro ao processar segmento SRT {index}: {e}")
                continue
        
        print(f"Parsed {len(segments)} segmentos do arquivo SRT")
        return segments
    
    @staticmethod
    def _clean_webvtt_text(text_block: str) -> str:
        """
        Limpa texto WEBVTT removendo tags e formatação
        
        Args:
            text_block: Bloco de texto bruto do WEBVTT
            
        Returns:
            str: Texto limpo
        """
        # Remove tags de formatação <c>, </c>, <00:00:01.234>, etc
        clean_text = re.sub(r'<[^>]*>', '', text_block)
        
        # Remove timestamps inline
        clean_text = re.sub(r'\d{2}:\d{2}:\d{2}\.\d{3}', '', clean_text)
        
        # Remove quebras de linha e espaços extras
        clean_text = re.sub(r'\s+', ' ', clean_text.strip())
        
        return clean_text
    
    @staticmethod
    def _webvtt_timestamp_to_seconds(timestamp_str: str) -> float:
        """
        Converte timestamp WEBVTT (HH:MM:SS.mmm) para segundos
        
        Args:
            timestamp_str: String no formato "00:01:23.456"
            
        Returns:
            float: Tempo em segundos (83.456)
        """
        # Formato WEBVTT: "00:01:23.456"
        time_part, ms_part = timestamp_str.split('.')
        hours, minutes, seconds = map(int, time_part.split(':'))
        
        total_seconds = hours * 3600 + minutes * 60 + seconds
        total_seconds += int(ms_part) / 1000.0
        
        return total_seconds
    
    @staticmethod
    def _timestamp_to_seconds(timestamp_str: str) -> float:
        """
        Converte timestamp SRT (HH:MM:SS,mmm) para segundos
        
        Args:
            timestamp_str: String no formato "00:01:23,456"
            
        Returns:
            float: Tempo em segundos (83.456)
        """
        # Formato: "00:01:23,456"
        time_part, ms_part = timestamp_str.split(',')
        hours, minutes, seconds = map(int, time_part.split(':'))
        
        total_seconds = hours * 3600 + minutes * 60 + seconds
        total_seconds += int(ms_part) / 1000.0
        
        return total_seconds


class SubtitleAligner:
    """
    Alinhador de legendas com segmentos de áudio
    Implementa alinhamento matemático baseado em duração temporal
    """
    
    def __init__(self):
        """Inicializa o alinhador"""
        self.parser = SubtitleParser()
    
    def _get_segment_original_time(self, segment_path: str, segments_dir: Path) -> Tuple[float, float]:
        """
        Calcula o tempo original do segmento no áudio completo
        Baseado na ordem sequencial dos segmentos e duração de cada um
        
        Args:
            segment_path: Caminho do arquivo de segmento
            segments_dir: Diretório contendo todos os segmentos
            
        Returns:
            Tuple[float, float]: (start_time, end_time) em segundos
        """
        segment_file = Path(segment_path)
        segment_name = segment_file.stem
        
        # Extrai índice do segmento (ex: "video_ID_5" -> 5)
        try:
            segment_index = int(segment_name.split('_')[-1])
        except:
            return (0.0, 0.0)
        
        # Calcula tempo acumulado dos segmentos anteriores
        accumulated_time = 0.0
        
        # Lista todos os segmentos em ordem
        all_segments = sorted(segments_dir.glob("*.wav"), 
                            key=lambda x: int(x.stem.split('_')[-1]))
        
        # Soma duração de todos os segmentos anteriores
        for seg_file in all_segments:
            seg_index = int(seg_file.stem.split('_')[-1])
            
            if seg_index >= segment_index:
                break
                
            # Calcula duração do segmento usando librosa
            try:
                duration = librosa.get_duration(path=str(seg_file))
                accumulated_time += duration
            except:
                # Fallback: estima 8 segundos por segmento (média)
                accumulated_time += 8.0
        
        # Calcula duração do segmento atual
        try:
            current_duration = librosa.get_duration(path=segment_path)
        except:
            current_duration = 8.0  # Fallback
        
        start_time = accumulated_time
        end_time = accumulated_time + current_duration
        
        return (start_time, end_time)
    
    def _find_matching_subtitle_text(self, start_time: float, end_time: float, 
                                   srt_segments: List[Dict], 
                                   config: AlignmentConfig) -> Optional[str]:
        """
        Encontra texto da legenda que corresponde ao período temporal
        
        Args:
            start_time: Início do segmento de áudio (segundos)
            end_time: Fim do segmento de áudio (segundos)
            srt_segments: Lista de segmentos da legenda
            config: Configurações de alinhamento
            
        Returns:
            str: Texto correspondente ou None se não encontrar
        """
        matching_texts = []
        
        # Busca todos os segmentos de legenda que se sobrepõem temporalmente
        for srt_seg in srt_segments:
            srt_start = srt_seg['start_time']
            srt_end = srt_seg['end_time']
            
            # Verifica sobreposição temporal com tolerância
            overlap_start = max(start_time, srt_start - config.time_tolerance_sec)
            overlap_end = min(end_time, srt_end + config.time_tolerance_sec)
            
            # Se há sobreposição significativa
            if overlap_end > overlap_start:
                overlap_duration = overlap_end - overlap_start
                segment_duration = end_time - start_time
                
                # Se sobreposição é significativa (pelo menos 30% do segmento)
                if overlap_duration >= (segment_duration * 0.3):
                    text = srt_seg['text']
                    
                    # Filtra textos muito curtos
                    if len(text.strip()) >= config.min_text_length:
                        matching_texts.append(text)
        
        # Combina textos encontrados
        if matching_texts:
            combined_text = ' '.join(matching_texts).strip()
            return combined_text if combined_text else None
        
        return None
    
    def _already_aligned(self, video_dir: Path) -> bool:
        """
        Verifica se alinhamento já foi executado para este vídeo
        Atualizado para verificar arquivo com nome específico do vídeo
        
        Args:
            video_dir: Diretório do vídeo
            
        Returns:
            bool: True se já foi alinhado
        """
        video_id = video_dir.name
        alignment_file = video_dir / f"{video_id}_alignment.json"
        return alignment_file.exists() and alignment_file.stat().st_size > 100
    
    def align_single_video(self, video_dir: str, 
                         config: AlignmentConfig = None,
                         overwrite: bool = False) -> Dict:
        """
        Alinha legendas com segmentos de um único vídeo
        
        Args:
            video_dir: Diretório contendo segmentos e arquivo .srt
            config: Configurações de alinhamento
            overwrite: Se True, realinha mesmo se já existir
            
        Returns:
            Dict: Resultado do alinhamento
        """
        if config is None:
            config = AlignmentConfig()
        
        video_path = Path(video_dir)
        
        # Verifica se já foi alinhado
        if not overwrite and self._already_aligned(video_path):
            return {
                "success": True,
                "skipped": True,
                "reason": "Já foi alinhado (use overwrite=True para forçar)",
                "video_dir": str(video_path)
            }
        
        # Busca arquivo .srt
        srt_files = list(video_path.glob("*.srt"))
        if not srt_files:
            return {
                "success": False,
                "error": "Arquivo .srt não encontrado",
                "video_dir": str(video_path)
            }
        
        srt_file = srt_files[0]  # Usa primeiro .srt encontrado
        
        # Busca pasta segments
        segments_dir = video_path / "segments"
        if not segments_dir.exists():
            return {
                "success": False,
                "error": "Pasta segments não encontrada",
                "video_dir": str(video_path)
            }
        
        try:
            print(f"Alinhando: {video_path.name}")
            
            # Parse do arquivo .srt
            srt_segments = self.parser.parse_srt_file(str(srt_file))
            
            if not srt_segments:
                return {
                    "success": False,
                    "error": "Falha ao fazer parse do arquivo .srt",
                    "video_dir": str(video_path)
                }
            
            # Lista todos os segmentos de áudio
            wav_files = sorted(segments_dir.glob("*.wav"), 
                             key=lambda x: int(x.stem.split('_')[-1]))
            
            if not wav_files:
                return {
                    "success": False,
                    "error": "Nenhum arquivo .wav encontrado em segments/",
                    "video_dir": str(video_path)
                }
            
            # Alinha cada segmento
            alignment_data = {}
            successful_alignments = 0
            orphan_segments = 0
            
            for wav_file in wav_files:
                # Calcula tempo original do segmento
                start_time, end_time = self._get_segment_original_time(
                    str(wav_file), segments_dir
                )
                
                # Busca texto correspondente na legenda
                matching_text = self._find_matching_subtitle_text(
                    start_time, end_time, srt_segments, config
                )
                
                segment_data = {
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": end_time - start_time
                }
                
                if matching_text:
                    segment_data["subtitle_text"] = matching_text
                    successful_alignments += 1
                else:
                    segment_data["subtitle_text"] = None
                    orphan_segments += 1
                    
                    if not config.allow_orphan_segments:
                        continue  # Pula segmentos sem texto
                
                alignment_data[wav_file.name] = segment_data
            
            # Salva resultado do alinhamento com nome específico do vídeo
            video_id = video_path.name
            output_file = video_path / f"{video_id}_alignment.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(alignment_data, f, indent=2, ensure_ascii=False)
            
            print(f"Alinhamento concluído: {successful_alignments} sucessos, {orphan_segments} órfãos")
            
            return {
                "success": True,
                "skipped": False,
                "video_dir": str(video_path),
                "total_segments": len(wav_files),
                "successful_alignments": successful_alignments,
                "orphan_segments": orphan_segments,
                "alignment_file": str(output_file),
                "srt_segments_parsed": len(srt_segments)
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Erro no alinhamento: {e}",
                "video_dir": str(video_path)
            }
    
    def align_batch_from_downloads(self, downloads_path: str = "downloads",
                                 config: AlignmentConfig = None,
                                 overwrite: bool = False) -> Dict:
        """
        Alinha todos os vídeos encontrados na pasta downloads
        Busca recursivamente por estruturas video_id/segments/
        
        Args:
            downloads_path: Caminho da pasta downloads
            config: Configurações de alinhamento
            overwrite: Se True, realinha mesmo se já existir
            
        Returns:
            Dict: Relatório consolidado do alinhamento em lote
        """
        if config is None:
            config = AlignmentConfig()
        
        downloads_dir = Path(downloads_path)
        
        if not downloads_dir.exists():
            return {
                "batch_completed": False,
                "error": f"Pasta downloads não encontrada: {downloads_path}"
            }
        
        # Busca todos os diretórios que contêm pasta segments
        video_dirs = []
        for segments_dir in downloads_dir.rglob("segments"):
            if segments_dir.is_dir():
                video_dir = segments_dir.parent
                video_dirs.append(video_dir)
        
        if not video_dirs:
            return {
                "batch_completed": False,
                "error": "Nenhum diretório com pasta segments encontrado"
            }
        
        print(f"Encontrados {len(video_dirs)} vídeos com segmentos para alinhar")
        print(f"Configuração overwrite: {overwrite}")
        
        results = []
        successful = 0
        failed = 0
        skipped = 0
        total_alignments = 0
        
        for i, video_dir in enumerate(video_dirs, 1):
            print(f"\n[{i}/{len(video_dirs)}] Processando: {video_dir.name}")
            
            result = self.align_single_video(str(video_dir), config, overwrite)
            results.append(result)
            
            if result['success']:
                if result.get('skipped', False):
                    skipped += 1
                    print(f"Pulado: {result['reason']}")
                else:
                    successful += 1
                    total_alignments += result['successful_alignments']
                    print(f"Sucesso: {result['successful_alignments']} alinhamentos")
            else:
                failed += 1
                print(f"Falha: {result['error']}")
        
        return {
            "batch_completed": True,
            "total_videos": len(video_dirs),
            "successful": successful,
            "failed": failed,
            "skipped": skipped,
            "total_alignments_created": total_alignments,
            "individual_results": results,
            "config_used": config
        }


def _find_project_root() -> Optional[Path]:
    """
    Encontra a raiz do projeto procurando pela pasta downloads
    Busca nos diretórios pai até encontrar
    
    Returns:
        Path: Caminho da raiz do projeto ou None se não encontrar
    """
    current = Path.cwd()
    
    # Busca até 4 níveis acima
    for _ in range(4):
        downloads_path = current / "downloads"
        if downloads_path.exists() and downloads_path.is_dir():
            return current
        current = current.parent
        
        if current == current.parent:
            break
    
    return None


# ========================================
# FUNÇÕES DE CONVENIÊNCIA PARA USO EXTERNO
# ========================================

def quick_align(video_dir: str, overwrite: bool = False) -> Dict:
    """
    Função de conveniência para alinhamento rápido de um vídeo
    
    Args:
        video_dir: Diretório do vídeo com segments/ e .srt
        overwrite: Se True, realinha mesmo se já existir
        
    Returns:
        Dict: Resultado do alinhamento
    """
    aligner = SubtitleAligner()
    return aligner.align_single_video(video_dir, overwrite=overwrite)


def align_all_downloads(overwrite: bool = False) -> Dict:
    """
    Alinha todos os vídeos na pasta downloads
    
    Args:
        overwrite: Se True, realinha arquivos já processados
        
    Returns:
        Dict: Relatório do processamento em lote
    """
    # Encontra raiz do projeto
    project_root = _find_project_root()
    
    if not project_root:
        return {
            "batch_completed": False,
            "error": "Pasta downloads não encontrada no projeto"
        }
    
    downloads_path = project_root / "downloads"
    aligner = SubtitleAligner()
    return aligner.align_batch_from_downloads(str(downloads_path), overwrite=overwrite)


# ========================================
# EXECUÇÃO STANDALONE PARA TESTES
# ========================================

def main():
    """
    Função principal para execução standalone
    Alinha todos os vídeos encontrados na pasta downloads
    """
    print("KATUBE SUBTITLE ALIGNER - Alinhamento Matemático")
    print("=" * 55)
    
    # Encontra e processa pasta downloads
    result = align_all_downloads(overwrite=False)
    
    if result.get('batch_completed'):
        print(f"\nAlinhamento concluído!")
        print(f"Vídeos processados: {result['successful']}")
        print(f"Vídeos pulados: {result['skipped']}")
        print(f"Falhas: {result['failed']}")
        print(f"Total de alinhamentos criados: {result['total_alignments_created']}")
        
        # Mostra onde foram salvos os arquivos
        if result['individual_results']:
            print(f"\nArquivos de alinhamento salvos como 'alignment_data.json' em cada pasta de vídeo")
    else:
        print(f"\nErro: {result.get('error', 'Erro desconhecido')}")


if __name__ == "__main__":
    main()