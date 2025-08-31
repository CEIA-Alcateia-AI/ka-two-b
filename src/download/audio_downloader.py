"""
Download de áudio do YouTube com validação e controle de qualidade
Implementa delays anti-bloqueio e verificações de integridade
"""
import os
import time
from random import randint
from pathlib import Path
from typing import List, Optional, Tuple

try:
    import yt_dlp
except ImportError:
    print("ERRO: yt-dlp não instalado")
    print("Execute: pip install yt-dlp")
    exit(1)

try:
    import mutagen
    from mutagen.mp3 import MP3
except ImportError:
    print("AVISO: mutagen não instalado - validação de duração desabilitada")
    print("Para habilitar: pip install mutagen")
    mutagen = None

from download_config import DownloadConfig


class AudioDownloader:
    """
    Downloader especializado em áudio do YouTube
    Implementa validações, delays anti-bloqueio e controle de qualidade
    """
    
    def __init__(self, config: DownloadConfig):
        """
        Inicializa downloader de áudio com configurações
        
        Args:
            config: Configuração do sistema de download
        """
        self.config = config
        self.stats = {
            'total_attempted': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'skipped_existing': 0,
            'validation_failures': 0
        }
    
    def download_audio_list(self, video_ids: List[str]) -> Tuple[List[str], List[str]]:
        """
        Baixa lista de áudios com delays anti-bloqueio
        
        Args:
            video_ids: Lista de IDs de vídeo para baixar
            
        Returns:
            Tuple[List[str], List[str]]: (sucessos, falhas)
        """
        successful = []
        failed = []
        
        print(f"Iniciando download de {len(video_ids)} áudios")
        print(f"Qualidade configurada: {self.config.audio_quality} kbps")
        
        for i, video_id in enumerate(video_ids, 1):
            print(f"\n[{i}/{len(video_ids)}] Processando: {video_id}")
            
            # Verifica se já existe
            if self.config.is_video_downloaded(video_id):
                print(f"Audio já existe: {video_id}")
                self.stats['skipped_existing'] += 1
                successful.append(video_id)
                continue
            
            # Tenta download
            self.stats['total_attempted'] += 1
            success = self._download_single_audio(video_id)
            
            if success:
                successful.append(video_id)
                self.stats['successful_downloads'] += 1
                print(f"SUCESSO: {video_id}")
            else:
                failed.append(video_id)
                self.stats['failed_downloads'] += 1
                print(f"FALHA: {video_id}")
            
            # Delay anti-bloqueio (exceto no último)
            if i < len(video_ids):
                delay = randint(*self.config.DELAYS['audio'])
                print(f"Aguardando {delay}s (anti-bloqueio)...")
                time.sleep(delay)
        
        self._print_audio_stats()
        return successful, failed
    
    def _download_single_audio(self, video_id: str) -> bool:
        """
        Baixa áudio de um vídeo específico
        
        Args:
            video_id: ID único do vídeo
            
        Returns:
            bool: True se download bem-sucedido
        """
        try:
            video_dir = self.config.get_video_output_path(video_id)
            video_dir.mkdir(parents=True, exist_ok=True)
            
            # Configurações específicas do yt-dlp para este vídeo
            ytdl_opts = self.config.get_ytdl_audio_options()
            
            # Ajusta template de saída para este vídeo específico
            ytdl_opts['outtmpl'] = str(video_dir / video_id)
            
            # URL do vídeo
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            
            print(f"Baixando áudio de: {video_url}")
            
            # Execute download
            with yt_dlp.YoutubeDL(ytdl_opts) as ydl:
                ydl.download([video_url])
            
            # Verifica se arquivo foi criado
            expected_file = self.config.get_audio_file_path(video_id)
            
            if not expected_file.exists():
                print(f"ERRO: Arquivo não foi criado - {expected_file}")
                return False
            
            # Validações de integridade
            if not self._validate_audio_file(expected_file):
                print(f"ERRO: Arquivo não passou na validação")
                return False
            
            print(f"Audio salvo: {expected_file}")
            return True
            
        except Exception as e:
            print(f"ERRO no download: {e}")
            return False
    
    def _validate_audio_file(self, audio_file: Path) -> bool:
        """
        Valida integridade e qualidade do arquivo de áudio baixado
        
        Args:
            audio_file: Caminho para arquivo de áudio
            
        Returns:
            bool: True se arquivo válido
        """
        # Validação 1: Arquivo existe e não está vazio
        if not audio_file.exists():
            print("VALIDAÇÃO: Arquivo não existe")
            return False
        
        file_size = audio_file.stat().st_size
        if file_size == 0:
            print("VALIDAÇÃO: Arquivo vazio")
            return False
        
        if file_size < 1024:  # Menor que 1KB é suspeito
            print(f"VALIDAÇÃO: Arquivo muito pequeno ({file_size} bytes)")
            return False
        
        print(f"VALIDAÇÃO: Tamanho OK ({file_size / (1024*1024):.1f} MB)")
        
        # Validação 2: Duração mínima (se mutagen disponível)
        if mutagen:
            try:
                audio = MP3(str(audio_file))
                duration = audio.info.length if audio.info else 0
                
                if duration < 45:  # Menos de 45 segundos
                    print(f"VALIDAÇÃO: Duração muito curta ({duration:.1f}s)")
                    self.stats['validation_failures'] += 1
                    return False
                
                print(f"VALIDAÇÃO: Duração OK ({duration:.1f}s)")
                
            except Exception as e:
                print(f"AVISO: Não foi possível validar duração - {e}")
                # Não falha se só a validação de duração der problema
        
        return True
    
    def _print_audio_stats(self) -> None:
        """Imprime estatísticas finais do download de áudio"""
        print("\n" + "="*50)
        print("ESTATISTICAS DE DOWNLOAD DE AUDIO")
        print("="*50)
        print(f"Total tentativas: {self.stats['total_attempted']}")
        print(f"Sucessos: {self.stats['successful_downloads']}")
        print(f"Falhas: {self.stats['failed_downloads']}")
        print(f"Já existiam: {self.stats['skipped_existing']}")
        print(f"Falhas de validação: {self.stats['validation_failures']}")
        
        if self.stats['total_attempted'] > 0:
            success_rate = (self.stats['successful_downloads'] / self.stats['total_attempted']) * 100
            print(f"Taxa de sucesso: {success_rate:.1f}%")
        
        print("="*50)
    
    def download_single_video(self, video_id: str) -> bool:
        """
        Conveniência: baixa áudio de um único vídeo
        
        Args:
            video_id: ID do vídeo
            
        Returns:
            bool: True se sucesso
        """
        successful, failed = self.download_audio_list([video_id])
        return len(successful) > 0
    
    def verify_downloaded_audios(self, video_ids: List[str]) -> Tuple[List[str], List[str]]:
        """
        Verifica quais áudios estão realmente baixados e válidos
        
        Args:
            video_ids: Lista de IDs para verificar
            
        Returns:
            Tuple[List[str], List[str]]: (válidos, inválidos)
        """
        valid = []
        invalid = []
        
        for video_id in video_ids:
            audio_file = self.config.get_audio_file_path(video_id)
            
            if self._validate_audio_file(audio_file):
                valid.append(video_id)
            else:
                invalid.append(video_id)
        
        print(f"Verificação: {len(valid)} válidos, {len(invalid)} inválidos")
        return valid, invalid


def download_audios_from_config(config: DownloadConfig, video_ids: List[str]) -> Tuple[List[str], List[str]]:
    """
    Função de conveniência para download de áudios a partir de configuração
    
    Args:
        config: Configuração do sistema
        video_ids: Lista de IDs para baixar
        
    Returns:
        Tuple[List[str], List[str]]: (sucessos, falhas)
    """
    downloader = AudioDownloader(config)
    return downloader.download_audio_list(video_ids)


def quick_audio_download(url: str, quality: int = 256) -> bool:
    """
    Download rápido de áudio para testes
    
    Args:
        url: URL do YouTube
        quality: Qualidade em kbps
        
    Returns:
        bool: True se sucesso
    """
    from download_config import quick_config
    from video_searcher import search_videos_from_config
    
    # Cria configuração
    config = quick_config(url, quality)
    
    # Busca vídeos
    video_ids = search_videos_from_config(config)
    
    if not video_ids:
        print("Nenhum vídeo encontrado")
        return False
    
    # Download de áudios
    successful, failed = download_audios_from_config(config, video_ids)
    
    return len(successful) > 0 and len(failed) == 0