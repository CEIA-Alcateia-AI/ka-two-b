"""
Configuração central do sistema de download YouTube
Detecção automática de tipo de URL e centralização de configurações
"""
import os
from urllib.parse import parse_qs, urlparse
from pathlib import Path
from typing import Tuple, Dict, List, Optional


class DownloadConfig:
    """
    Configuração centralizada para sistema de download YouTube
    Detecta automaticamente tipo de URL e fornece configurações para todos os módulos
    """
    
    # Configurações fixas do desenvolvedor
    AUDIO_QUALITIES = [192, 256, 320]  # kbps disponíveis para usuário
    SUBTITLE_PRIORITY = ['manual-pt-br', 'manual-pt', 'auto-pt-br', 'auto-pt']
    DELAYS = {
        'audio': (15, 40),      # segundos entre downloads de áudio  
        'subtitle': (5, 20)     # segundos entre downloads de legenda
    }
    
    # Chave API YouTube (será configurável via Streamlit)
    DEFAULT_API_KEY = "AIzaSyBJ2b2eT8onlm5dt7WHaHZyHEsmnIkpfbk"
    
    # Estrutura de pastas
    BASE_OUTPUT_DIR = "downloads"
    
    def __init__(self, url: str, audio_quality: int = 256, api_key: Optional[str] = None):
        """
        Inicializa configuração com URL fornecida
        
        Args:
            url: URL do YouTube (canal, playlist ou vídeo)
            audio_quality: Qualidade em kbps (192, 256, 320)
            api_key: Chave da API YouTube (usa padrão se None)
        """
        self.original_url = url
        self.audio_quality = self._validate_audio_quality(audio_quality)
        self.api_key = api_key or self.DEFAULT_API_KEY
        
        # Detecção automática de tipo e ID
        self.url_type, self.content_id = self._detect_url_type(url)
        
        # Configurações derivadas
        self.output_dir = self._create_output_path()
        self.youtube_videos_file = self.output_dir / "youtube_videos.txt"
        self.error_log_file = self.output_dir / "error_youtube_videos.json"
        self.downloaded_log_file = self.output_dir / "downloaded_youtube_videos.txt"
        
        # Garante que diretório existe
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _validate_audio_quality(self, quality: int) -> int:
        """Valida qualidade de áudio escolhida"""
        if quality not in self.AUDIO_QUALITIES:
            print(f"Qualidade {quality} inválida. Usando 256 kbps por padrão.")
            return 256
        return quality
    
    def _detect_url_type(self, url: str) -> Tuple[str, str]:
        """
        Detecta automaticamente tipo de URL e extrai ID único
        
        Returns:
            tuple: (tipo, id) onde tipo é 'channel', 'playlist' ou 'video'
        """
        # Playlist
        if 'playlist?list=' in url or '&list=' in url:
            playlist_id = parse_qs(urlparse(url).query).get('list', [None])[0]
            if playlist_id:
                return 'playlist', playlist_id
        
        # Canal por handle (@usuario)
        if '/@' in url:
            handle = url.split('/@')[1].split('/')[0].split('?')[0]
            return 'channel', f"@{handle}"
        
        # Canal por ID (UCxxxxx)
        if '/channel/' in url:
            channel_id = url.split('/channel/')[1].split('/')[0].split('?')[0]
            return 'channel', channel_id
        
        # Canal por user
        if '/user/' in url:
            user_id = url.split('/user/')[1].split('/')[0].split('?')[0]
            return 'channel', f"user_{user_id}"
        
        # Canal por nome customizado (youtube.com/c/nome)
        if '/c/' in url:
            custom_name = url.split('/c/')[1].split('/')[0].split('?')[0]
            return 'channel', f"c_{custom_name}"
        
        # Vídeo individual
        if 'watch?v=' in url:
            video_id = parse_qs(urlparse(url).query).get('v', [None])[0]
            if video_id:
                return 'video', video_id
        
        # URL encurtada do YouTube
        if 'youtu.be/' in url:
            video_id = url.split('youtu.be/')[1].split('?')[0]
            return 'video', video_id
        
        raise ValueError(f"Tipo de URL não reconhecido: {url}")
    
    def _create_output_path(self) -> Path:
        """Cria caminho de saída baseado no tipo e ID detectados"""
        base_dir = Path(self.BASE_OUTPUT_DIR)
        
        if self.url_type == 'video':
            # Vídeos individuais vão em pasta separada
            return base_dir / f"video_{self.content_id}"
        else:
            # Canais e playlists usam prefixo + ID
            return base_dir / f"{self.url_type}_{self.content_id}"
    
    def get_video_output_path(self, video_id: str) -> Path:
        """
        Retorna caminho completo para arquivos de um vídeo específico
        
        Args:
            video_id: ID único do vídeo
            
        Returns:
            Path: Caminho para pasta do vídeo
        """
        return self.output_dir / video_id
    
    def get_audio_file_path(self, video_id: str) -> Path:
        """Retorna caminho completo para arquivo de áudio"""
        return self.get_video_output_path(video_id) / f"{video_id}.mp3"
    
    def get_subtitle_file_path(self, video_id: str) -> Path:
        """Retorna caminho completo para arquivo de legenda"""
        return self.get_video_output_path(video_id) / f"{video_id}.srt"
    
    def is_video_downloaded(self, video_id: str) -> bool:
        """
        Verifica se vídeo já foi baixado (evita duplicatas)
        
        Args:
            video_id: ID do vídeo a verificar
            
        Returns:
            bool: True se áudio já existe
        """
        audio_file = self.get_audio_file_path(video_id)
        return audio_file.exists()
    
    def get_ytdl_audio_options(self) -> Dict:
        """
        Retorna configurações do yt-dlp para download de áudio
        
        Returns:
            dict: Configurações otimizadas para yt-dlp
        """
        return {
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': str(self.audio_quality),
            }],
            'outtmpl': str(self.get_video_output_path('%(id)s') / '%(id)s'),
            'quiet': True,
            'no_warnings': True,
            'extractaudio': True,
            'audioformat': 'mp3',
        }
    
    def get_subtitle_languages(self) -> List[str]:
        """
        Retorna lista de idiomas para busca de legendas
        Prioridade: português brasileiro > português genérico
        
        Returns:
            list: Lista de códigos de idioma em ordem de prioridade
        """
        return ['pt', 'pt-br', 'pt-BR']
    
    def create_streamlit_summary(self) -> Dict:
        """
        Cria resumo das configurações para exibição no Streamlit
        
        Returns:
            dict: Dados formatados para interface
        """
        return {
            'url_original': self.original_url,
            'tipo_detectado': self.url_type,
            'content_id': self.content_id,
            'qualidade_audio': f"{self.audio_quality} kbps",
            'pasta_saida': str(self.output_dir),
            'arquivos_controle': {
                'lista_videos': str(self.youtube_videos_file),
                'log_erros': str(self.error_log_file),
                'log_baixados': str(self.downloaded_log_file)
            }
        }
    
    def __str__(self) -> str:
        """Representação string para debug"""
        return f"DownloadConfig(tipo={self.url_type}, id={self.content_id}, qualidade={self.audio_quality}kbps)"


def create_config_from_streamlit(**kwargs) -> DownloadConfig:
    """
    Factory function para criar configuração a partir de parâmetros do Streamlit
    
    Args:
        **kwargs: Parâmetros vindos da interface Streamlit
                 - url (str): URL do YouTube
                 - audio_quality (int): Qualidade do áudio
                 - api_key (str, opcional): Chave da API
    
    Returns:
        DownloadConfig: Instância configurada
    """
    required_params = ['url']
    for param in required_params:
        if param not in kwargs:
            raise ValueError(f"Parâmetro obrigatório '{param}' não fornecido")
    
    return DownloadConfig(
        url=kwargs['url'],
        audio_quality=kwargs.get('audio_quality', 256),
        api_key=kwargs.get('api_key', None)
    )


# Função de conveniência para teste rápido
def quick_config(url: str, quality: int = 256) -> DownloadConfig:
    """
    Cria configuração rapidamente para testes
    
    Args:
        url: URL do YouTube
        quality: Qualidade do áudio (padrão 256)
    
    Returns:
        DownloadConfig: Configuração pronta para uso
    """
    return DownloadConfig(url=url, audio_quality=quality)