"""
Busca de vídeos do YouTube - canais, playlists e vídeos individuais
Gera arquivo youtube_videos.txt seguindo padrão do projeto original
"""
import sys
from typing import List, Optional
from pathlib import Path
from urllib.parse import parse_qs, urlparse

try:
    from googleapiclient.discovery import build
except ImportError:
    print("ERRO: google-api-python-client não instalado")
    print("Execute: pip install google-api-python-client")
    sys.exit(1)

try:
    import yt_dlp
except ImportError:
    print("ERRO: yt-dlp não instalado")
    print("Execute: pip install yt-dlp")
    sys.exit(1)

from download_config import DownloadConfig


class VideoSearcher:
    """
    Busca vídeos do YouTube e gera lista para download
    Suporta canais, playlists e vídeos individuais
    """
    
    def __init__(self, config: DownloadConfig):
        """
        Inicializa buscador com configurações
        
        Args:
            config: Configuração do sistema de download
        """
        self.config = config
        self.youtube_api = None
        
        # Inicializa API do YouTube se chave disponível
        if config.api_key:
            try:
                self.youtube_api = build('youtube', 'v3', developerKey=config.api_key)
                print(f"API YouTube inicializada com sucesso")
            except Exception as e:
                print(f"Aviso: Falha ao inicializar API YouTube: {e}")
                print("Usando método alternativo (yt-dlp) - mais lento mas funciona")
    
    def search_videos(self) -> List[str]:
        """
        Busca lista completa de vídeos baseada na configuração
        
        Returns:
            List[str]: Lista de URLs dos vídeos encontrados
        """
        # Verifica se já existe arquivo de vídeos (evita re-buscar)
        if self.config.youtube_videos_file.exists():
            print(f"Arquivo {self.config.youtube_videos_file.name} já existe")
            return self._load_existing_videos()
        
        print(f"Buscando vídeos do tipo: {self.config.url_type}")
        print(f"ID: {self.config.content_id}")
        
        # Busca baseada no tipo detectado
        if self.config.url_type == 'video':
            video_urls = [self.config.original_url]
        elif self.config.url_type == 'playlist':
            video_urls = self._search_playlist_videos()
        elif self.config.url_type == 'channel':
            video_urls = self._search_channel_videos()
        else:
            raise ValueError(f"Tipo de URL não suportado: {self.config.url_type}")
        
        if not video_urls:
            print("ERRO: Nenhum vídeo encontrado")
            return []
        
        # Salva lista no arquivo (padrão original)
        self._save_videos_list(video_urls)
        
        print(f"Total de vídeos encontrados: {len(video_urls)}")
        return video_urls
    
    def _load_existing_videos(self) -> List[str]:
        """Carrega lista de vídeos de arquivo existente"""
        try:
            with open(self.config.youtube_videos_file, 'r', encoding='utf-8') as f:
                video_urls = [line.strip() for line in f.readlines() if line.strip()]
            
            print(f"Carregados {len(video_urls)} vídeos do arquivo existente")
            return video_urls
            
        except Exception as e:
            print(f"Erro ao carregar arquivo existente: {e}")
            return []
    
    def _search_playlist_videos(self) -> List[str]:
        """Busca vídeos de uma playlist"""
        playlist_id = self.config.content_id
        
        # Tenta primeiro com API oficial (mais rápido)
        if self.youtube_api:
            try:
                return self._search_playlist_with_api(playlist_id)
            except Exception as e:
                print(f"Falha na API, usando yt-dlp: {e}")
        
        # Fallback: usa yt-dlp (mais lento mas confiável)
        return self._search_playlist_with_ytdlp()
    
    def _search_channel_videos(self) -> List[str]:
        """Busca vídeos de um canal"""
        # Para canais, sempre usa yt-dlp pois é mais confiável
        # A API oficial requer conversão canal -> playlist de uploads
        return self._search_channel_with_ytdlp()
    
    def _search_playlist_with_api(self, playlist_id: str) -> List[str]:
        """Busca playlist usando API oficial do YouTube"""
        print("Usando API oficial do YouTube...")
        
        video_urls = []
        next_page_token = None
        
        while True:
            # Busca vídeos da playlist (50 por página)
            request = self.youtube_api.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            
            response = request.execute()
            
            # Extrai URLs dos vídeos
            for item in response.get('items', []):
                video_id = item['snippet']['resourceId']['videoId']
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                video_urls.append(video_url)
            
            # Verifica se há próxima página
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
            
            print(f"Processando página... Total atual: {len(video_urls)}")
        
        return video_urls
    
    def _search_playlist_with_ytdlp(self) -> List[str]:
        """Busca playlist usando yt-dlp (fallback)"""
        print("Usando yt-dlp para extrair playlist...")
        
        try:
            ydl_opts = {
                'quiet': True,
                'extract_flat': True,  # Só extrai URLs, não baixa
                'playlistreverse': False,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(self.config.original_url, download=False)
                
                if 'entries' in info and info['entries']:
                    video_urls = []
                    for entry in info['entries']:
                        if entry and entry.get('id'):
                            video_url = f"https://www.youtube.com/watch?v={entry['id']}"
                            video_urls.append(video_url)
                    return video_urls
                
            return []
            
        except Exception as e:
            print(f"Erro ao buscar playlist com yt-dlp: {e}")
            return []
    
    def _search_channel_with_ytdlp(self) -> List[str]:
        """Busca canal usando yt-dlp"""
        print("Usando yt-dlp para extrair vídeos do canal...")
        
        try:
            ydl_opts = {
                'quiet': True,
                'extract_flat': True,
                'playlistreverse': False,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(self.config.original_url, download=False)
                
                if 'entries' in info and info['entries']:
                    video_urls = []
                    for entry in info['entries']:
                        if entry and entry.get('id'):
                            video_url = f"https://www.youtube.com/watch?v={entry['id']}"
                            video_urls.append(video_url)
                    return video_urls
                
            return []
            
        except Exception as e:
            print(f"Erro ao buscar canal com yt-dlp: {e}")
            return []
    
    def _save_videos_list(self, video_urls: List[str]) -> None:
        """
        Salva lista de vídeos no arquivo (padrão do projeto original)
        
        Args:
            video_urls: Lista de URLs para salvar
        """
        try:
            with open(self.config.youtube_videos_file, 'w', encoding='utf-8') as f:
                for url in video_urls:
                    f.write(url + '\n')
            
            print(f"Lista salva em: {self.config.youtube_videos_file}")
            
        except Exception as e:
            print(f"Erro ao salvar lista de vídeos: {e}")
            raise
    
    def get_video_ids_from_urls(self, video_urls: List[str]) -> List[str]:
        """
        Extrai video_ids das URLs para uso no download
        
        Args:
            video_urls: Lista de URLs completas
            
        Returns:
            List[str]: Lista de video_ids únicos
        """
        video_ids = []
        
        for url in video_urls:
            try:
                # Extrai video_id da URL
                parsed = urlparse(url)
                
                if 'youtube.com' in parsed.netloc:
                    # URL padrão: youtube.com/watch?v=VIDEO_ID
                    video_id = parse_qs(parsed.query).get('v', [None])[0]
                elif 'youtu.be' in parsed.netloc:
                    # URL encurtada: youtu.be/VIDEO_ID
                    video_id = parsed.path.lstrip('/')
                else:
                    print(f"URL não reconhecida: {url}")
                    continue
                
                if video_id and video_id not in video_ids:
                    video_ids.append(video_id)
                    
            except Exception as e:
                print(f"Erro ao processar URL {url}: {e}")
                continue
        
        return video_ids
    
    def filter_new_videos(self, video_ids: List[str]) -> List[str]:
        """
        Remove vídeos já baixados da lista (evita duplicatas)
        
        Args:
            video_ids: Lista completa de video_ids
            
        Returns:
            List[str]: Lista filtrada (só vídeos não baixados)
        """
        new_videos = []
        skipped_count = 0
        
        for video_id in video_ids:
            if not self.config.is_video_downloaded(video_id):
                new_videos.append(video_id)
            else:
                skipped_count += 1
        
        if skipped_count > 0:
            print(f"Ignorando {skipped_count} vídeos já baixados")
        
        print(f"Vídeos novos para download: {len(new_videos)}")
        return new_videos


def search_videos_from_config(config: DownloadConfig) -> List[str]:
    """
    Função de conveniência para buscar vídeos a partir de configuração
    
    Args:
        config: Configuração do sistema
        
    Returns:
        List[str]: Lista de video_ids prontos para download
    """
    searcher = VideoSearcher(config)
    
    # Busca URLs completas
    video_urls = searcher.search_videos()
    
    if not video_urls:
        return []
    
    # Converte URLs para video_ids
    video_ids = searcher.get_video_ids_from_urls(video_urls)
    
    # Filtra vídeos já baixados
    new_video_ids = searcher.filter_new_videos(video_ids)
    
    return new_video_ids