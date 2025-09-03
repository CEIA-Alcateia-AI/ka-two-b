#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modulo de Alinhamento de Legendas para Segmentos de Audio
Mapeia legendas WebVTT/SRT completas para segmentos de audio individuais
Preparado para integracao com Streamlit e pipeline modular
"""

import os
import re
import json
import csv
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import unicodedata


@dataclass
class AlignmentConfig:
    """
    Configuracoes para alinhamento de legendas
    Parametros baseados em benchmarks da industria TTS/STT
    """
    # Qualidade minima do texto
    min_text_length: int = 10          # Caracteres minimos por segmento
    max_text_length: int = 200         # Caracteres maximos por segmento
    
    # Configuracao de confianca (ajustado para dados reais)
    min_confidence_score: float = 0.6  # Score minimo para incluir no dataset
    
    # Normalizacao de texto
    normalize_text: bool = True        # Aplicar normalizacao automatica
    remove_timestamps_text: bool = True # Remover marcadores temporais do texto


class SubtitleAligner:
    """
    Alinhador de legendas para segmentos de audio
    Implementa mapeamento proporcional entre legenda completa e segmentos VAD
    """
    
    def __init__(self, config: Optional[AlignmentConfig] = None):
        """Inicializa o alinhador com configuracao padrao"""
        self.config = config or AlignmentConfig()
        self.alignment_stats = {
            'total_segments_processed': 0,
            'successful_alignments': 0,
            'low_confidence_segments': 0,
            'empty_text_segments': 0
        }
    
    def parse_webvtt_file(self, srt_file_path: str) -> List[Dict]:
        """
        Processa arquivo WebVTT/SRT extraindo timestamps e texto
        Trata formato hibrido comum no yt-dlp (WebVTT com extensao .srt)
        
        Args:
            srt_file_path: Caminho para arquivo de legenda
            
        Returns:
            List[Dict]: Lista com dados das legendas
        """
        if not os.path.exists(srt_file_path):
            print(f"Arquivo de legenda nao encontrado: {srt_file_path}")
            return []
        
        try:
            with open(srt_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            print(f"Processando arquivo de legenda: {os.path.basename(srt_file_path)}")
            
            # Detecta formato (WebVTT ou SRT tradicional)
            if 'WEBVTT' in content:
                return self._parse_webvtt_content(content)
            else:
                return self._parse_srt_content(content)
                
        except Exception as e:
            print(f"Erro ao processar arquivo de legenda: {e}")
            return []
    
    def _parse_webvtt_content(self, content: str) -> List[Dict]:
        """
        Parser especifico para formato WebVTT do YouTube
        Baseado na estrutura vista na imagem fornecida
        """
        subtitle_entries = []
        
        # Remove cabecalho WebVTT e metadados
        lines = content.split('\n')
        text_lines = []
        
        # Filtra apenas linhas com timestamps e texto
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.startswith(('WEBVTT', 'Kind:', 'Language:')):
                continue
            if 'align:start position:' in line and '-->' in line:
                # Linha de timestamp WebVTT: "00:00:00.320 --> 00:00:02.070 align:start position:0%"
                timestamp_match = re.match(r'(\d{2}:\d{2}:\d{2}\.\d{3})\s+-->\s+(\d{2}:\d{2}:\d{2}\.\d{3})', line)
                if timestamp_match:
                    start_time = self._parse_timestamp(timestamp_match.group(1))
                    end_time = self._parse_timestamp(timestamp_match.group(2))
                    text_lines.append({
                        'start_time': start_time,
                        'end_time': end_time,
                        'text': ''  # Texto vira na proxima linha
                    })
            elif text_lines and not re.match(r'\d{2}:\d{2}:\d{2}', line):
                # Linha de texto que segue timestamp
                if text_lines[-1]['text']:
                    text_lines[-1]['text'] += ' ' + line
                else:
                    text_lines[-1]['text'] = line
        
        # Converte para formato padrao
        for entry in text_lines:
            if entry['text'].strip():
                subtitle_entries.append({
                    'start': entry['start_time'],
                    'end': entry['end_time'],
                    'duration': entry['end_time'] - entry['start_time'],
                    'text': self._normalize_subtitle_text(entry['text'])
                })
        
        print(f"WebVTT: {len(subtitle_entries)} entradas de legenda extraidas")
        return subtitle_entries
    
    def _parse_srt_content(self, content: str) -> List[Dict]:
        """
        Parser para formato SRT tradicional (fallback)
        """
        subtitle_entries = []
        
        # Regex para blocos SRT tradicionais
        srt_pattern = re.compile(
            r'(\d+)\s*\n'                           # Numero da legenda
            r'(\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*'   # Timestamp inicio  
            r'(\d{2}:\d{2}:\d{2},\d{3})\s*\n'       # Timestamp fim
            r'(.*?)\n\n',                           # Texto (pode ser multilinha)
            re.DOTALL
        )
        
        matches = srt_pattern.findall(content)
        
        for match in matches:
            start_time = self._parse_timestamp(match[1].replace(',', '.'))
            end_time = self._parse_timestamp(match[2].replace(',', '.'))
            text = match[3].strip()
            
            if text:
                subtitle_entries.append({
                    'start': start_time,
                    'end': end_time,
                    'duration': end_time - start_time,
                    'text': self._normalize_subtitle_text(text)
                })
        
        print(f"SRT: {len(subtitle_entries)} entradas de legenda extraidas")
        return subtitle_entries
    
    def _parse_timestamp(self, timestamp_str: str) -> float:
        """
        Converte timestamp string para segundos (float)
        Suporta formatos: HH:MM:SS.mmm e HH:MM:SS,mmm
        """
        # Normaliza separador decimal
        timestamp_str = timestamp_str.replace(',', '.')
        
        # Parse HH:MM:SS.mmm
        time_parts = timestamp_str.split(':')
        if len(time_parts) == 3:
            hours = int(time_parts[0])
            minutes = int(time_parts[1])
            seconds_parts = time_parts[2].split('.')
            seconds = int(seconds_parts[0])
            milliseconds = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
            
            return hours * 3600 + minutes * 60 + seconds + milliseconds / 1000
        
        return 0.0
    
    def _normalize_subtitle_text(self, text: str) -> str:
        """
        Normaliza texto da legenda aplicando limpeza padrao
        Remove tags HTML, normaliza espacos, caracteres especiais
        """
        if not self.config.normalize_text:
            return text.strip()
        
        # Remove tags HTML comuns em legendas
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove quebras de linha e espacos excessivos  
        text = re.sub(r'\s+', ' ', text)
        
        # Remove marcadores temporais que podem aparecer no texto
        if self.config.remove_timestamps_text:
            text = re.sub(r'\d{1,2}:\d{2}:\d{2}', '', text)
            text = re.sub(r'\[\d+:\d+\]', '', text)
        
        # Normaliza acentos e caracteres especiais
        text = unicodedata.normalize('NFKC', text)
        
        return text.strip()
    
    def get_audio_segments_info(self, segments_dir: str) -> List[Dict]:
        """
        Extrai informacoes dos segmentos de audio existentes
        Baseado na estrutura padrao criada pelo AudioSegmenter
        
        Args:
            segments_dir: Diretorio com arquivos .wav segmentados
            
        Returns:
            List[Dict]: Info de cada segmento (nome, duracao estimada, indice)
        """
        segments_path = Path(segments_dir)
        
        if not segments_path.exists():
            print(f"Diretorio de segmentos nao encontrado: {segments_dir}")
            return []
        
        # Busca arquivos .wav seguindo padrao video_id_N.wav
        wav_files = sorted(segments_path.glob("*.wav"))
        
        segments_info = []
        for wav_file in wav_files:
            # Extrai indice do nome do arquivo (ex: VmEu1gB3lXc_0.wav -> 0)
            filename = wav_file.stem
            parts = filename.split('_')
            
            if len(parts) >= 2 and parts[-1].isdigit():
                segment_index = int(parts[-1])
                segments_info.append({
                    'filename': wav_file.name,
                    'filepath': str(wav_file),
                    'index': segment_index,
                    'estimated_duration': 4.0  # Duracao media estimada (4-15s conforme config VAD)
                })
        
        print(f"Encontrados {len(segments_info)} segmentos de audio")
        return segments_info
    
    def align_subtitles_to_segments(self, subtitle_entries: List[Dict], 
                                  segments_info: List[Dict]) -> Dict:
        """
        Alinha legendas aos segmentos usando distribuicao proporcional inteligente
        Estrategia: mapear texto total proporcionalmente aos segmentos existentes
        
        Args:
            subtitle_entries: Dados das legendas extraidas
            segments_info: Informacoes dos segmentos de audio
            
        Returns:
            Dict: Dados de alinhamento estruturados
        """
        if not subtitle_entries or not segments_info:
            print("Dados insuficientes para alinhamento")
            return {'segments': {}, 'stats': self.alignment_stats}
        
        # Combina todo o texto das legendas
        full_text = ' '.join([entry['text'] for entry in subtitle_entries if entry['text']])
        total_duration = max([entry['end'] for entry in subtitle_entries]) if subtitle_entries else 0
        
        print(f"Texto total: {len(full_text)} caracteres")
        print(f"Duracao total estimada: {total_duration:.2f} segundos")
        print(f"Segmentos para mapear: {len(segments_info)}")
        
        # Divide texto proporcionalmente entre segmentos
        alignment_data = {'segments': {}}
        words = full_text.split()
        words_per_segment = max(1, len(words) // len(segments_info))
        
        current_word_pos = 0
        
        for i, segment in enumerate(segments_info):
            self.alignment_stats['total_segments_processed'] += 1
            
            # Calcula porcao de palavras para este segmento
            if i == len(segments_info) - 1:  # Ultimo segmento pega o resto
                segment_words = words[current_word_pos:]
            else:
                segment_words = words[current_word_pos:current_word_pos + words_per_segment]
            
            # Calcula score de confianca baseado em heuristicas
            confidence_score = self._calculate_confidence_score(segment_text, segment)
            
            print(f"DEBUG Segmento {segment['filename']}: texto={len(segment_text)} chars, palavras={len(segment_words)}, confianca={confidence_score:.3f}")
            
            # Valida qualidade do texto
            if (len(segment_text) >= self.config.min_text_length and 
                len(segment_text) <= self.config.max_text_length and
                confidence_score >= self.config.min_confidence_score):
                
                alignment_data['segments'][segment['filename']] = {
                    'start_time': i * (total_duration / len(segments_info)),
                    'end_time': (i + 1) * (total_duration / len(segments_info)),
                    'duration': segment['estimated_duration'],
                    'subtitle_text': segment_text,
                    'alignment_confidence': confidence_score,
                    'segment_index': segment['index']
                }
                
                self.alignment_stats['successful_alignments'] += 1
                
            else:
                # Registra segmento com baixa qualidade
                if len(segment_text) < self.config.min_text_length:
                    self.alignment_stats['empty_text_segments'] += 1
                else:
                    self.alignment_stats['low_confidence_segments'] += 1
                
                print(f"Segmento {segment['filename']}: REJEITADO - tamanho={len(segment_text)}, confianca={confidence_score:.3f}, threshold={self.config.min_confidence_score}")
        
        # Adiciona estatisticas ao resultado
        alignment_data['video_id'] = self._extract_video_id_from_segments(segments_info)
        alignment_data['total_segments'] = len(segments_info)
        alignment_data['successful_alignments'] = self.alignment_stats['successful_alignments']
        alignment_data['alignment_method'] = 'proportional_distribution'
        alignment_data['stats'] = self.alignment_stats
        
        print(f"Alinhamento concluido: {self.alignment_stats['successful_alignments']}/{len(segments_info)} segmentos")
        
        return alignment_data
    
    def _find_natural_text_break(self, full_text: str, start_pos: int, target_end: int) -> str:
        """
        Encontra quebra natural no texto (espaco, ponto, virgula)
        para evitar cortar palavras no meio
        """
        if target_end >= len(full_text):
            return full_text[start_pos:].strip()
        
        # Busca quebra natural em janela de +/- 50 caracteres
        search_window = 50
        best_break = target_end
        
        # Procura por pontos, virgulas, espacos (ordem de preferencia)
        for char in ['.', '!', '?', ',', ' ']:
            # Busca para tras
            for i in range(min(search_window, target_end - start_pos)):
                pos = target_end - i
                if pos > start_pos and full_text[pos] == char:
                    best_break = pos + 1
                    break
            if best_break != target_end:
                break
        
        return full_text[start_pos:best_break].strip()
    
    def _calculate_confidence_score(self, text: str, segment_info: Dict) -> float:
        """
        Calcula score de confianca baseado em heuristicas de qualidade
        Score varia de 0.0 a 1.0
        """
        score = 1.0
        
        # Penaliza textos muito curtos ou muito longos
        text_len = len(text)
        if text_len < self.config.min_text_length:
            score *= 0.3
        elif text_len > self.config.max_text_length:
            score *= 0.7
        
        # Penaliza textos com muitos caracteres especiais (possiveis artefatos)
        special_chars = len(re.findall(r'[^\w\s\.,!?;:]', text))
        if special_chars > text_len * 0.1:  # Mais de 10% caracteres especiais
            score *= 0.6
        
        # Bonus para textos com pontuacao adequada
        if any(punct in text for punct in ['.', '!', '?', ',']):
            score *= 1.1
        
        # Bonus para textos com palavras completas (nao cortados)
        words = text.split()
        if len(words) >= 2:  # Pelo menos 2 palavras
            score *= 1.05
        
        return min(score, 1.0)  # Limita a 1.0
    
    def _extract_video_id_from_segments(self, segments_info: List[Dict]) -> str:
        """
        Extrai video_id a partir dos nomes dos arquivos de segmento
        Baseado no padrao video_id_N.wav
        """
        if not segments_info:
            return 'unknown'
        
        first_segment = segments_info[0]['filename']
        # Remove extensao e numero do segmento
        video_id = '_'.join(first_segment.split('_')[:-1])
        return video_id.replace('.wav', '')
    
    def create_dataset_files(self, alignment_data: Dict, output_dir: str) -> Dict:
        """
        Gera arquivos finais do dataset no formato padrao da industria
        Cria: arquivos .txt individuais + metadata.csv + alignment_data.json
        
        Args:
            alignment_data: Dados de alinhamento estruturados
            output_dir: Diretorio onde salvar os arquivos
            
        Returns:
            Dict: Relatorio dos arquivos criados
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        created_files = {
            'txt_files': [],
            'metadata_csv': '',
            'alignment_json': '',
            'stats': {}
        }
        
        segments_data = alignment_data.get('segments', {})
        
        if not segments_data:
            print("Nenhum segmento alinhado para gerar dataset")
            return created_files
        
        print(f"Gerando dataset com {len(segments_data)} segmentos...")
        
        # 1. Cria arquivos .txt individuais para cada segmento
        for wav_filename, segment_data in segments_data.items():
            txt_filename = wav_filename.replace('.wav', '.txt')
            txt_filepath = output_path / txt_filename
            
            try:
                with open(txt_filepath, 'w', encoding='utf-8') as f:
                    f.write(segment_data['subtitle_text'])
                
                created_files['txt_files'].append(str(txt_filepath))
                
            except Exception as e:
                print(f"Erro ao criar {txt_filename}: {e}")
        
        # 2. Cria metadata.csv no formato LJSpeech
        metadata_file = output_path / 'metadata.csv'
        try:
            with open(metadata_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='|')
                
                # Header opcional (descomente se necessario)
                # writer.writerow(['filename', 'text', 'duration', 'confidence'])
                
                for wav_filename, segment_data in segments_data.items():
                    writer.writerow([
                        wav_filename.replace('.wav', ''),  # Nome sem extensao
                        segment_data['subtitle_text'],
                        f"{segment_data['duration']:.2f}",
                        f"{segment_data['alignment_confidence']:.3f}"
                    ])
            
            created_files['metadata_csv'] = str(metadata_file)
            print(f"Metadata CSV criado: {metadata_file}")
            
        except Exception as e:
            print(f"Erro ao criar metadata.csv: {e}")
        
        # 3. Salva dados completos de alinhamento em JSON
        alignment_file = output_path / 'alignment_data.json'
        try:
            with open(alignment_file, 'w', encoding='utf-8') as f:
                json.dump(alignment_data, f, indent=2, ensure_ascii=False)
            
            created_files['alignment_json'] = str(alignment_file)
            print(f"Dados de alinhamento salvos: {alignment_file}")
            
        except Exception as e:
            print(f"Erro ao criar alignment_data.json: {e}")
        
        # 4. Gera estatisticas do dataset criado
        created_files['stats'] = {
            'total_txt_files': len(created_files['txt_files']),
            'metadata_entries': len(segments_data),
            'average_confidence': sum(s['alignment_confidence'] for s in segments_data.values()) / len(segments_data),
            'total_text_characters': sum(len(s['subtitle_text']) for s in segments_data.values()),
            'output_directory': str(output_path)
        }
        
        print(f"Dataset criado com sucesso:")
        print(f"  - {created_files['stats']['total_txt_files']} arquivos .txt")
        print(f"  - 1 arquivo metadata.csv")  
        print(f"  - 1 arquivo alignment_data.json")
        print(f"  - Confianca media: {created_files['stats']['average_confidence']:.3f}")
        
        return created_files
    
    def align_srt_to_segments(self, srt_file: str, segments_dir: str) -> Dict:
        """
        Metodo principal: alinha arquivo SRT/WebVTT aos segmentos de audio
        Interface simplificada para uso externo
        
        Args:
            srt_file: Caminho para arquivo de legenda (.srt)
            segments_dir: Diretorio com segmentos de audio (.wav)
            
        Returns:
            Dict: Dados de alinhamento completos
        """
        print("Iniciando alinhamento de legendas aos segmentos de audio")
        print(f"Arquivo de legenda: {os.path.basename(srt_file)}")
        print(f"Diretorio de segmentos: {segments_dir}")
        
        # 1. Parse das legendas
        subtitle_entries = self.parse_webvtt_file(srt_file)
        
        if not subtitle_entries:
            return {'error': 'Nao foi possivel extrair legendas do arquivo'}
        
        # 2. Informacoes dos segmentos de audio
        segments_info = self.get_audio_segments_info(segments_dir)
        
        if not segments_info:
            return {'error': 'Nenhum segmento de audio encontrado no diretorio'}
        
        # 3. Alinhamento proporcional
        alignment_result = self.align_subtitles_to_segments(subtitle_entries, segments_info)
        
        return alignment_result


# ========================================
# FUNCOES DE CONVENIENCIA PARA USO EXTERNO  
# ========================================

def quick_align(srt_file: str, segments_dir: str, 
                output_dir: Optional[str] = None) -> Dict:
    """
    Alinhamento rapido para testes e uso simples
    
    Args:
        srt_file: Caminho para arquivo de legenda
        segments_dir: Diretorio com segmentos de audio
        output_dir: Diretorio de saida (usa segments_dir se None)
        
    Returns:
        Dict: Relatorio completo do alinhamento
    """
    if output_dir is None:
        output_dir = segments_dir
    
    aligner = SubtitleAligner()
    
    # Executa alinhamento
    alignment_result = aligner.align_srt_to_segments(srt_file, segments_dir)
    
    if 'error' in alignment_result:
        return alignment_result
    
    # Gera arquivos do dataset
    dataset_result = aligner.create_dataset_files(alignment_result, output_dir)
    
    # Combina resultados
    return {
        'alignment_successful': True,
        'alignment_data': alignment_result,
        'dataset_files': dataset_result,
        'summary': {
            'segments_aligned': alignment_result.get('successful_alignments', 0),
            'total_segments': alignment_result.get('total_segments', 0),
            'output_directory': output_dir
        }
    }


def align_from_video_directory(video_dir: str) -> Dict:
    """
    Alinha legendas aos segmentos dentro de um diretorio de video
    Funciona com estrutura padrao: video_dir/segments/ + video_dir/video_id.srt
    
    Args:
        video_dir: Diretorio do video (contem .srt e pasta segments/)
        
    Returns:
        Dict: Relatorio do alinhamento
    """
    video_path = Path(video_dir)
    
    if not video_path.exists():
        return {'error': f'Diretorio nao encontrado: {video_dir}'}
    
    # Busca arquivo SRT no diretorio
    srt_files = list(video_path.glob("*.srt"))
    if not srt_files:
        return {'error': 'Nenhum arquivo .srt encontrado no diretorio'}
    
    srt_file = str(srt_files[0])  # Usa primeiro arquivo .srt encontrado
    
    # Busca diretorio de segmentos
    segments_dir = video_path / 'segments'
    if not segments_dir.exists():
        return {'error': 'Diretorio segments/ nao encontrado'}
    
    print(f"Processando video: {video_path.name}")
    
    # Executa alinhamento rapido
    return quick_align(srt_file, str(segments_dir), str(segments_dir))


# ========================================
# EXECUCAO STANDALONE PARA TESTES
# ========================================

def _find_test_data() -> Optional[Tuple[str, str]]:
    """
    Procura dados de teste disponiveis na estrutura do projeto
    Baseado nos caminhos vistos nas imagens
    """
    # Possiveis localizacoes dos dados de teste
    test_paths = [
        "downloads/playlist_PLd6DVnxUXB2yi_HtPcjZdxA0Kna6K8-Ng/VmEu1gB3lXc",
        "../downloads/playlist_PLd6DVnxUXB2yi_HtPcjZdxA0Kna6K8-Ng/VmEu1gB3lXc", 
        "../../downloads/playlist_PLd6DVnxUXB2yi_HtPcjZdxA0Kna6K8-Ng/VmEu1gB3lXc"
    ]
    
    for test_path in test_paths:
        video_dir = Path(test_path)
        if video_dir.exists():
            srt_file = video_dir / "VmEu1gB3lXc.srt"
            segments_dir = video_dir / "segments"
            
            if srt_file.exists() and segments_dir.exists():
                print(f"Dados de teste encontrados: {video_dir}")
                return str(srt_file), str(segments_dir)
    
    return None


def main():
    """
    Funcao principal para execucao standalone
    Testa alinhamento com dados disponiveis no projeto
    """
    print("KATUBE SUBTITLE ALIGNER - Teste de Alinhamento")
    print("=" * 60)
    
    # Procura dados de teste
    test_data = _find_test_data()
    
    if test_data:
        srt_file, segments_dir = test_data
        print(f"Testando alinhamento:")
        print(f"  Legenda: {os.path.basename(srt_file)}")
        print(f"  Segmentos: {segments_dir}")
        
        # Executa teste de alinhamento
        result = quick_align(srt_file, segments_dir)
        
        if result.get('alignment_successful'):
            summary = result['summary']
            print(f"\nAlinhamento concluido com sucesso!")
            print(f"  Segmentos alinhados: {summary['segments_aligned']}/{summary['total_segments']}")
            print(f"  Arquivos criados em: {summary['output_directory']}")
            
            # Lista arquivos criados
            dataset_files = result.get('dataset_files', {})
            if dataset_files.get('txt_files'):
                print(f"  Arquivos .txt: {len(dataset_files['txt_files'])}")
            if dataset_files.get('metadata_csv'):
                print(f"  Metadata CSV: {os.path.basename(dataset_files['metadata_csv'])}")
                
        else:
            print(f"\nErro no alinhamento: {result.get('error', 'Erro desconhecido')}")
            
    else:
        print("Nenhum dado de teste encontrado")
        print("Execute primeiro o modulo de download ou verifique a estrutura do projeto")
        print("\nEstrutura esperada:")
        print("  downloads/playlist_*/video_id/video_id.srt")
        print("  downloads/playlist_*/video_id/segments/*.wav")


if __name__ == "__main__":
    main()