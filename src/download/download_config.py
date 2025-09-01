#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configurações centralizadas para download de áudio e legendas do YouTube
Compatível com pipeline de dataset para TTS/STT via Streamlit
"""

import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from typing import Dict, List, Optional


class DownloadConfig:
    """
    Configuração centralizada para download YouTube usando yt-dlp
    Otimizada para simplicidade e compatibilidade com Streamlit
    """
    
    # ========================================
    # CONFIGURAÇÕES PRINCIPAIS DO USUÁRIO
    # ========================================
    
    # URL alvo para download (vídeo, playlist ou canal)
    TARGET_URL = "https://www.youtube.com/watch?v=rI8m_dbr1gA&list=PLd6DVnxUXB2yi_HtPcjZdxA0Kna6K8-Ng&pp=gAQB"
    
    # Limite de downloads (0 = todos os vídeos)
    DOWNLOAD_LIMIT = 0 # Máximo de vídeos para baixar
    
    # Configurações de áudio
    AUDIO_FORMAT = "mp3"     # Opções: mp3, wav, m4a, flac
    AUDIO_QUALITY = 0      # kbps: 128, 192, 256, 320 (0 = melhor disponível)
    
    # Delays anti-bloqueio (segundos)
    DELAY_MIN_SECONDS = 15   # Mínimo entre downloads
    DELAY_MAX_SECONDS = 30   # Máximo entre downloads
    
    # Configuração de legendas (ordem de prioridade)
    SUBTITLE_LANGUAGES = ["pt-BR", "pt"]  # Manual pt-BR > Manual pt > Auto pt-BR > Auto pt
    DOWNLOAD_AUTO_SUBS = True             # Baixa legendas automáticas se manual não existir
    
    # Estrutura de output
    BASE_OUTPUT_DIR = "./downloads"       # Pasta base dos downloads
    
    # ========================================
    # CONFIGURAÇÕES AVANÇADAS (OPCIONAL)
    # ========================================
    
    # Filtros de duração de vídeo
    MIN_DURATION_SECONDS = 30      # Mínimo: 30 segundos
    MAX_DURATION_SECONDS = 3600    # Máximo: 1 hora
    
    # Comportamento de sobrescrita
    OVERWRITE_EXISTING = False     # True = redownload, False = pula existentes
    
    # Manter arquivo de vídeo também
    KEEP_VIDEO_FILE = False        # True = mantém .mp4, False = só áudio
    
    # ========================================
    # CONFIGURAÇÕES INTERNAS (NÃO ALTERAR)
    # ========================================
    
    def __init__(self, target_url: Optional[str] = None):
        """
        Inicializa configuração com URL opcional
        
        Args:
            target_url: URL para override da configuração padrão
        """
        self.target_url = target_url or self.TARGET_URL
        self.url_type, self.content_id = self._detect_url_type(self.target_url)
        self.output_dir = self._create_output_path()
        
        # Arquivos de controle
        self.videos_list_file = self.output_dir / "youtube_videos.txt"
        self.error_log_file = self.output_dir / "download_errors.json"
        self.success_log_file = self.output_dir / "download_success.json"
        
        # Garante que diretório existe
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _detect_url_type(self, url: str) -> tuple:
        """
        Detecta tipo de URL YouTube e extrai ID único
        
        Returns:
            tuple: (tipo, id) - 'channel', 'playlist' ou 'video'
        """
        # Playlist
        if 'playlist?list=' in url or '&list=' in url:
            playlist_id = parse_qs(urlparse(url).query).get('list', [None])[0]
            if playlist_id:
                return 'playlist', playlist_id
        
        # Canal por handle (@usuario)
        if '/@' in url:
            handle = url.split('/@')[1].split('/')[0].split('?')[0]
            return 'channel', handle
        
        # Canal por ID (UC...)
        if '/channel/' in url:
            channel_id = url.split('/channel/')[1].split('/')[0].split('?')[0]
            return 'channel', channel_id
        
        # Vídeo individual
        if 'watch?v=' in url:
            video_id = parse_qs(urlparse(url).query).get('v', [None])[0]
            if video_id:
                return 'video', video_id
        
        # URL encurtada
        if 'youtu.be/' in url:
            video_id = url.split('youtu.be/')[1].split('?')[0]
            return 'video', video_id
        
        raise ValueError(f"Tipo de URL não reconhecido: {url}")
    
    def _create_output_path(self) -> Path:
        """
        Cria caminho de saída baseado no tipo e ID detectados
        Estrutura: downloads/tipo_id/
        """
        base_dir = Path(self.BASE_OUTPUT_DIR)
        
        if self.url_type == 'video':
            return base_dir / f"video_{self.content_id}"
        else:
            return base_dir / f"{self.url_type}_{self.content_id}"
    
    def get_video_output_path(self, video_id: str) -> Path:
        """
        Retorna caminho completo para arquivos de um vídeo
        Estrutura: downloads/tipo_id/video_id/
        """
        return self.output_dir / video_id
    
    def get_audio_file_path(self, video_id: str) -> Path:
        """Retorna caminho para arquivo de áudio"""
        extension = self.AUDIO_FORMAT
        return self.get_video_output_path(video_id) / f"{video_id}.{extension}"
    
    def get_subtitle_file_path(self, video_id: str) -> Path:
        """Retorna caminho para arquivo de legenda"""
        return self.get_video_output_path(video_id) / f"{video_id}.srt"
    
    def is_video_downloaded(self, video_id: str) -> bool:
        """
        Verifica se vídeo já foi baixado (evita duplicatas)
        Lógica preservada do sistema original
        
        Returns:
            bool: True se áudio já existe
        """
        audio_file = self.get_audio_file_path(video_id)
        return audio_file.exists() and audio_file.stat().st_size > 1024  # Mínimo 1KB
    
    def get_ytdlp_command_args(self) -> List[str]:
        """
        Constrói argumentos para comando yt-dlp
        Unifica download de áudio e legendas em um comando
        
        Returns:
            List[str]: Lista de argumentos para subprocess
        """
        args = [
            "yt-dlp",
            
            # Extração de áudio
            "-x",  # Extrair áudio apenas
            "--audio-format", self.AUDIO_FORMAT,
            "--audio-quality", str(self.AUDIO_QUALITY),
            
            # Download de legendas
            "--write-subs",      # Legendas manuais
            "--write-auto-subs", # Legendas automáticas
            "--sub-lang", ",".join(self.SUBTITLE_LANGUAGES),
            
            # Template de output (estrutura de pastas)
            "--output", str(self.output_dir / "%(id)s" / "%(id)s.%(ext)s"),
            
            # Controle de download
            "--ignore-errors",   # Continua mesmo com erros
            "--no-warnings",     # Reduz logs desnecessários
        ]
        
        # Adiciona limite se configurado
        if self.DOWNLOAD_LIMIT > 0:
            args.extend(["--playlist-end", str(self.DOWNLOAD_LIMIT)])
        
        # Adiciona filtros de duração
        if self.MIN_DURATION_SECONDS > 0:
            args.extend(["--match-filter", f"duration >= {self.MIN_DURATION_SECONDS}"])
        
        if self.MAX_DURATION_SECONDS > 0:
            args.extend(["--match-filter", f"duration <= {self.MAX_DURATION_SECONDS}"])
        
        # Adiciona URL alvo
        args.append(self.target_url)
        
        return args
    
    def validate_config(self) -> Dict[str, any]:
        """
        Valida configurações e retorna relatório
        
        Returns:
            Dict: Relatório de validação com erros/avisos
        """
        validation = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Valida qualidade de áudio
        valid_qualities = [0, 128, 192, 256, 320]
        if self.AUDIO_QUALITY not in valid_qualities:
            validation['errors'].append(f"Qualidade inválida: {self.AUDIO_QUALITY}. Use: {valid_qualities}")
            validation['valid'] = False
        
        # Valida formato de áudio
        valid_formats = ["mp3", "wav", "m4a", "flac"]
        if self.AUDIO_FORMAT not in valid_formats:
            validation['errors'].append(f"Formato inválido: {self.AUDIO_FORMAT}. Use: {valid_formats}")
            validation['valid'] = False
        
        # Valida delays
        if self.DELAY_MIN_SECONDS > self.DELAY_MAX_SECONDS:
            validation['errors'].append("DELAY_MIN deve ser menor que DELAY_MAX")
            validation['valid'] = False
        
        # Avisos úteis
        if self.DOWNLOAD_LIMIT == 0:
            validation['warnings'].append("DOWNLOAD_LIMIT = 0: Baixará TODOS os vídeos")
        
        if self.AUDIO_QUALITY == 0:
            validation['warnings'].append("AUDIO_QUALITY = 0: Usará melhor qualidade disponível")
        
        return validation
    
    def create_summary(self) -> Dict:
        """
        Cria resumo das configurações para exibição
        Preparado para interface Streamlit
        
        Returns:
            Dict: Resumo formatado das configurações
        """
        return {
            'url_original': self.target_url,
            'tipo_detectado': self.url_type,
            'content_id': self.content_id,
            'limite_downloads': self.DOWNLOAD_LIMIT if self.DOWNLOAD_LIMIT > 0 else "Todos",
            'qualidade_audio': f"{self.AUDIO_QUALITY} kbps" if self.AUDIO_QUALITY > 0 else "Melhor disponível",
            'formato_audio': self.AUDIO_FORMAT.upper(),
            'delay_range': f"{self.DELAY_MIN_SECONDS}-{self.DELAY_MAX_SECONDS}s",
            'linguagens_legenda': " > ".join(self.SUBTITLE_LANGUAGES),
            'pasta_saida': str(self.output_dir),
            'arquivos_controle': {
                'lista_videos': str(self.videos_list_file),
                'log_erros': str(self.error_log_file),
                'log_sucessos': str(self.success_log_file)
            }
        }


# ========================================
# CONFIGURAÇÕES ADICIONAIS PARA USUÁRIO
# ========================================

# Configurações de filtro de conteúdo
CONTENT_FILTERS = {
    'skip_live_streams': True,      # Pula transmissões ao vivo
    'skip_premieres': False,        # Pula premieres
    'skip_shorts': False,           # Pula YouTube Shorts
}

# Configurações de retry
RETRY_CONFIG = {
    'max_retries': 3,               # Tentativas por vídeo
    'retry_delay': 60,              # Segundos entre tentativas
}

# Configurações de limpeza automática
CLEANUP_CONFIG = {
    'remove_temp_files': True,      # Remove arquivos temporários
    'remove_failed_downloads': True, # Remove downloads incompletos
}


def create_config_instance(url: Optional[str] = None) -> DownloadConfig:
    """
    Factory function para criar instância de configuração
    
    Args:
        url: URL opcional para override
        
    Returns:
        DownloadConfig: Instância configurada
    """
    return DownloadConfig(target_url=url)


def print_example_usage():
    """
    Imprime exemplos de uso das configurações
    Útil para documentação e debug
    """
    print("="*60)
    print("EXEMPLOS DE CONFIGURAÇÃO")
    print("="*60)
    print("# Para playlist pequena (teste):")
    print('TARGET_URL = "https://youtube.com/playlist?list=PLxxx"')
    print("DOWNLOAD_LIMIT = 5")
    print("AUDIO_QUALITY = 192")
    print()
    print("# Para canal completo (produção):")
    print('TARGET_URL = "https://youtube.com/@canal"')
    print("DOWNLOAD_LIMIT = 0  # Todos os vídeos")
    print("AUDIO_QUALITY = 320  # Máxima qualidade")
    print()
    print("# Para vídeo individual:")
    print('TARGET_URL = "https://youtube.com/watch?v=VIDEO_ID"')
    print("DOWNLOAD_LIMIT = 1")
    print("="*60)


if __name__ == "__main__":
    # Teste básico das configurações
    config = create_config_instance()
    
    print("Testando configurações...")
    validation = config.validate_config()
    
    if validation['valid']:
        print("✅ Configurações válidas!")
        summary = config.create_summary()
        
        print("\nResumo das configurações:")
        for key, value in summary.items():
            print(f"  {key}: {value}")
    else:
        print("❌ Erros encontrados:")
        for error in validation['errors']:
            print(f"  - {error}")
    
    print("\n")
    print_example_usage()