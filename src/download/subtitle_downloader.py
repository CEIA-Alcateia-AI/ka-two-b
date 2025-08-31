"""
Download de legendas do YouTube usando yt-dlp
Solução robusta que funciona com legendas manuais e automáticas
"""
import time
import subprocess
import os
from random import randint
from pathlib import Path
from typing import List, Tuple, Optional

try:
    import yt_dlp
except ImportError:
    print("ERRO: yt-dlp não instalado")
    print("Execute: pip install yt-dlp")
    exit(1)

from download_config import DownloadConfig


class SubtitleDownloader:
    """
    Downloader de legendas usando yt-dlp
    Mais confiável que youtube-transcript-api
    """
    
    def __init__(self, config: DownloadConfig):
        """
        Inicializa downloader de legendas
        
        Args:
            config: Configuração do sistema de download
        """
        self.config = config
        self.stats = {
            'total_attempted': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'skipped_existing': 0,
            'manual_subtitles': 0,
            'auto_subtitles': 0,
            'no_subtitles_available': 0
        }
    
    def download_subtitles_list(self, video_ids: List[str]) -> Tuple[List[str], List[str]]:
        """
        Baixa legendas para lista de vídeos (após áudios prontos)
        
        Args:
            video_ids: Lista de IDs com áudios já baixados
            
        Returns:
            Tuple[List[str], List[str]]: (sucessos, falhas)
        """
        successful = []
        failed = []
        
        print(f"Iniciando download de legendas para {len(video_ids)} vídeos")
        print("Usando yt-dlp para máxima compatibilidade")
        
        for i, video_id in enumerate(video_ids, 1):
            print(f"\n[{i}/{len(video_ids)}] Legendas para: {video_id}")
            
            # Verifica se áudio existe (pré-requisito)
            audio_file = self.config.get_audio_file_path(video_id)
            if not audio_file.exists():
                print(f"PULANDO: Áudio não existe para {video_id}")
                failed.append(video_id)
                continue
            
            # Verifica se legenda já existe
            subtitle_file = self.config.get_subtitle_file_path(video_id)
            if subtitle_file.exists():
                print(f"Legenda já existe: {video_id}")
                self.stats['skipped_existing'] += 1
                successful.append(video_id)
                continue
            
            # Tenta download de legenda
            self.stats['total_attempted'] += 1
            success = self._download_single_subtitle(video_id)
            
            if success:
                successful.append(video_id)
                self.stats['successful_downloads'] += 1
                print(f"SUCESSO: Legenda para {video_id}")
            else:
                failed.append(video_id)
                self.stats['failed_downloads'] += 1
                print(f"SEM LEGENDA: {video_id}")
            
            # Delay menor (legendas são operação mais leve)
            if i < len(video_ids):
                delay = randint(*self.config.DELAYS['subtitle'])
                print(f"Aguardando {delay}s...")
                time.sleep(delay)
        
        self._print_subtitle_stats()
        return successful, failed
    
    def _download_single_subtitle(self, video_id: str) -> bool:
        """
        Baixa legenda com hierarquia de prioridade:
        1. Manual pt-BR, 2. Manual pt, 3. Automática pt-BR, 4. Automática pt
        
        Args:
            video_id: ID único do vídeo
            
        Returns:
            bool: True se legenda encontrada e baixada
        """
        try:
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            video_dir = self.config.get_video_output_path(video_id)
            
            # Tenta hierarquia de prioridade
            subtitle_configs = [
                # Prioridade 1: Manual pt-BR
                {
                    'writesubtitles': True,
                    'writeautomaticsubs': False,
                    'subtitleslangs': ['pt-BR'],
                    'type': 'manual_pt-BR'
                },
                # Prioridade 2: Manual pt
                {
                    'writesubtitles': True,
                    'writeautomaticsubs': False,
                    'subtitleslangs': ['pt'],
                    'type': 'manual_pt'
                },
                # Prioridade 3: Automática pt-BR
                {
                    'writesubtitles': False,
                    'writeautomaticsubs': True,
                    'subtitleslangs': ['pt-BR'],
                    'type': 'auto_pt-BR'
                },
                # Prioridade 4: Automática pt
                {
                    'writesubtitles': False,
                    'writeautomaticsubs': True,
                    'subtitleslangs': ['pt'],
                    'type': 'auto_pt'
                },
            ]
            
            print(f"Baixando legendas de: {video_url}")
            
            for config in subtitle_configs:
                print(f"Tentando: {config['type']}")
                
                # Configuração base do yt-dlp
                ydl_opts = {
                    'writesubtitles': config['writesubtitles'],
                    'writeautomaticsubs': config['writeautomaticsubs'],
                    'subtitleslangs': config['subtitleslangs'],
                    'skip_download': True,
                    'outtmpl': str(video_dir / video_id),
                    'quiet': True,
                    'no_warnings': True,
                }
                
                try:
                    # Tenta download com esta configuração
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        ydl.download([video_url])
                    
                    # Verifica se arquivo foi baixado
                    subtitle_path = self._find_downloaded_subtitle(video_dir, video_id)
                    
                    if subtitle_path:
                        # Padroniza nome do arquivo
                        final_subtitle = self.config.get_subtitle_file_path(video_id)
                        
                        if subtitle_path != final_subtitle:
                            subtitle_path.rename(final_subtitle)
                        
                        # Atualiza estatísticas baseado no tipo
                        if 'manual' in config['type']:
                            self.stats['manual_subtitles'] += 1
                        else:
                            self.stats['auto_subtitles'] += 1
                        
                        print(f"SUCESSO: Legenda {config['type']} salva: {final_subtitle}")
                        return True
                
                except Exception as e:
                    print(f"Falha em {config['type']}: {e}")
                    continue
            
            # Se chegou aqui, nenhuma configuração funcionou
            print("Nenhuma legenda PT encontrada em qualquer modalidade")
            self.stats['no_subtitles_available'] += 1
            return False
                
        except Exception as e:
            print(f"ERRO geral ao baixar legenda: {e}")
            return False
    
    def _find_downloaded_subtitle(self, video_dir: Path, video_id: str) -> Optional[Path]:
        """
        Procura arquivo de legenda baixado pelo yt-dlp
        
        Args:
            video_dir: Diretório do vídeo
            video_id: ID do vídeo
            
        Returns:
            Path: Caminho do arquivo encontrado ou None
        """
        # Padrões de arquivo que yt-dlp pode gerar
        patterns = [
            f"{video_id}.pt.vtt",      # Legenda automática PT
            f"{video_id}.pt-BR.vtt",   # Legenda manual PT-BR
            f"{video_id}.pt.srt",      # SRT português
            f"{video_id}.pt-BR.srt",   # SRT português Brasil
        ]
        
        for pattern in patterns:
            subtitle_file = video_dir / pattern
            if subtitle_file.exists():
                print(f"Encontrou arquivo: {subtitle_file}")
                return subtitle_file
        
        # Busca qualquer arquivo de legenda português
        for file in video_dir.glob("*.vtt"):
            if 'pt' in file.name.lower():
                print(f"Encontrou arquivo genérico PT: {file}")
                return file
        
        for file in video_dir.glob("*.srt"):
            if 'pt' in file.name.lower():
                print(f"Encontrou arquivo genérico PT: {file}")
                return file
        
        return None
    
    def _detect_subtitle_type(self, filename: str) -> str:
        """
        Detecta se legenda é manual ou automática pelo nome
        
        Args:
            filename: Nome do arquivo de legenda
            
        Returns:
            str: 'manual' ou 'auto'
        """
        filename_lower = filename.lower()
        
        # Indicadores de legenda automática
        auto_indicators = ['auto', 'generated', 'automatic']
        
        for indicator in auto_indicators:
            if indicator in filename_lower:
                return 'auto'
        
        # Se tem código específico de região (pt-BR), tende a ser manual
        if 'pt-br' in filename_lower:
            return 'manual'
        
        # Padrão: assumir automática se não tiver indicador claro
        return 'auto'
    
    def _print_subtitle_stats(self) -> None:
        """Imprime estatísticas de download de legendas"""
        print("\n" + "="*50)
        print("ESTATISTICAS DE DOWNLOAD DE LEGENDAS")
        print("="*50)
        print(f"Total tentativas: {self.stats['total_attempted']}")
        print(f"Sucessos: {self.stats['successful_downloads']}")
        print(f"Falhas: {self.stats['failed_downloads']}")
        print(f"Já existiam: {self.stats['skipped_existing']}")
        print(f"Legendas manuais: {self.stats['manual_subtitles']}")
        print(f"Legendas automáticas: {self.stats['auto_subtitles']}")
        print(f"Sem legendas PT: {self.stats['no_subtitles_available']}")
        
        if self.stats['total_attempted'] > 0:
            success_rate = (self.stats['successful_downloads'] / self.stats['total_attempted']) * 100
            print(f"Taxa de sucesso: {success_rate:.1f}%")
        
        print("="*50)
    
    def download_single_subtitle(self, video_id: str) -> bool:
        """
        Conveniência: baixa legenda de um único vídeo
        
        Args:
            video_id: ID do vídeo (deve ter áudio já baixado)
            
        Returns:
            bool: True se sucesso
        """
        successful, failed = self.download_subtitles_list([video_id])
        return len(successful) > 0


def download_subtitles_from_config(config: DownloadConfig, video_ids: List[str]) -> Tuple[List[str], List[str]]:
    """
    Função de conveniência para download de legendas
    
    Args:
        config: Configuração do sistema
        video_ids: Lista de IDs com áudios já baixados
        
    Returns:
        Tuple[List[str], List[str]]: (sucessos, falhas)
    """
    downloader = SubtitleDownloader(config)
    return downloader.download_subtitles_list(video_ids)