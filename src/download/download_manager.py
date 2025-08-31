"""
Orquestrador principal do sistema de download YouTube
Interface única para Streamlit - coordena pipeline completo
"""
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from download_config import DownloadConfig, create_config_from_streamlit
from video_searcher import VideoSearcher
from audio_downloader import AudioDownloader
from subtitle_downloader import SubtitleDownloader


class DownloadManager:
    """
    Gerenciador principal do sistema de download
    Coordena pipeline completo: busca → áudio → legendas
    Interface preparada para Streamlit
    """
    
    def __init__(self, config: DownloadConfig):
        """
        Inicializa gerenciador com configuração
        
        Args:
            config: Configuração completa do sistema
        """
        self.config = config
        self.searcher = VideoSearcher(config)
        self.audio_downloader = AudioDownloader(config)
        self.subtitle_downloader = SubtitleDownloader(config)
        
        # Estatísticas consolidadas
        self.pipeline_stats = {
            'started_at': datetime.now(),
            'finished_at': None,
            'total_execution_time': 0,
            'videos_found': 0,
            'videos_processed': 0,
            'audio_successful': 0,
            'audio_failed': 0,
            'subtitle_successful': 0,
            'subtitle_failed': 0,
            'complete_pairs': 0  # áudio + legenda
        }
        
        # Logs detalhados
        self.error_log = []
        self.success_log = []
    
    def execute_full_pipeline(self) -> Dict:
        """
        Executa pipeline completo de download
        
        Returns:
            Dict: Relatório completo da execução
        """
        print("="*60)
        print("INICIANDO PIPELINE COMPLETO DE DOWNLOAD")
        print("="*60)
        print(f"URL: {self.config.original_url}")
        print(f"Tipo: {self.config.url_type}")
        print(f"Qualidade áudio: {self.config.audio_quality} kbps")
        print(f"Pasta saída: {self.config.output_dir}")
        
        try:
            # FASE 1: Busca de vídeos
            print("\n[FASE 1] Buscando lista de vídeos...")
            video_ids = self._execute_video_search()
            
            if not video_ids:
                return self._create_final_report("Nenhum vídeo encontrado")
            
            # FASE 2: Download de áudios
            print(f"\n[FASE 2] Baixando {len(video_ids)} áudios...")
            audio_successful, audio_failed = self._execute_audio_downloads(video_ids)
            
            # FASE 3: Download de legendas (só para áudios baixados)
            print(f"\n[FASE 3] Baixando legendas para {len(audio_successful)} vídeos...")
            subtitle_successful, subtitle_failed = self._execute_subtitle_downloads(audio_successful)
            
            # FASE 4: Relatório final
            return self._create_final_report("Pipeline concluído", {
                'audio_successful': audio_successful,
                'audio_failed': audio_failed,
                'subtitle_successful': subtitle_successful,
                'subtitle_failed': subtitle_failed
            })
            
        except Exception as e:
            return self._create_final_report(f"Erro no pipeline: {e}")
    
    def _execute_video_search(self) -> List[str]:
        """Executa busca e retorna lista de video_ids"""
        video_urls = self.searcher.search_videos()
        
        if not video_urls:
            return []
        
        video_ids = self.searcher.get_video_ids_from_urls(video_urls)
        new_video_ids = self.searcher.filter_new_videos(video_ids)
        
        self.pipeline_stats['videos_found'] = len(video_ids)
        self.pipeline_stats['videos_processed'] = len(new_video_ids)
        
        return new_video_ids
    
    def _execute_audio_downloads(self, video_ids: List[str]) -> Tuple[List[str], List[str]]:
        """Executa downloads de áudio com estatísticas"""
        successful, failed = self.audio_downloader.download_audio_list(video_ids)
        
        # Consolida estatísticas
        self.pipeline_stats['audio_successful'] = len(successful)
        self.pipeline_stats['audio_failed'] = len(failed)
        
        # Log de erros de áudio
        for video_id in failed:
            self.error_log.append({
                'video_id': video_id,
                'phase': 'audio_download',
                'error': 'download_failed',
                'timestamp': datetime.now().isoformat()
            })
        
        return successful, failed
    
    def _execute_subtitle_downloads(self, video_ids: List[str]) -> Tuple[List[str], List[str]]:
        """Executa downloads de legendas para vídeos com áudio"""
        if not video_ids:
            print("Nenhum áudio baixado - pulando legendas")
            return [], []
        
        successful, failed = self.subtitle_downloader.download_subtitles_list(video_ids)
        
        # Consolida estatísticas
        self.pipeline_stats['subtitle_successful'] = len(successful)
        self.pipeline_stats['subtitle_failed'] = len(failed)
        
        # Calcula pares completos (áudio + legenda)
        self.pipeline_stats['complete_pairs'] = len(successful)
        
        # Log de sucessos completos
        for video_id in successful:
            self.success_log.append({
                'video_id': video_id,
                'has_audio': True,
                'has_subtitle': True,
                'timestamp': datetime.now().isoformat()
            })
        
        # Log de áudios sem legenda
        for video_id in failed:
            self.success_log.append({
                'video_id': video_id,
                'has_audio': True,
                'has_subtitle': False,
                'timestamp': datetime.now().isoformat()
            })
        
        return successful, failed
    
    def _create_final_report(self, status: str, details: Optional[Dict] = None) -> Dict:
        """Cria relatório final da execução"""
        self.pipeline_stats['finished_at'] = datetime.now()
        
        if self.pipeline_stats['started_at']:
            elapsed = self.pipeline_stats['finished_at'] - self.pipeline_stats['started_at']
            self.pipeline_stats['total_execution_time'] = elapsed.total_seconds()
        
        report = {
            'status': status,
            'config': self.config.create_streamlit_summary(),
            'stats': self.pipeline_stats,
            'details': details or {},
            'error_log': self.error_log,
            'success_log': self.success_log
        }
        
        # Salva relatório em arquivo JSON
        self._save_execution_report(report)
        
        # Imprime resumo no terminal
        self._print_pipeline_summary()
        
        return report
    
    def _save_execution_report(self, report: Dict) -> None:
        """Salva relatório de execução em arquivo JSON"""
        try:
            report_file = self.config.output_dir / "execution_report.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"Relatório salvo: {report_file}")
            
        except Exception as e:
            print(f"Erro ao salvar relatório: {e}")
    
    def _print_pipeline_summary(self) -> None:
        """Imprime resumo final do pipeline"""
        stats = self.pipeline_stats
        
        print("\n" + "="*60)
        print("RELATÓRIO FINAL DO PIPELINE")
        print("="*60)
        print(f"Tempo total: {stats['total_execution_time']:.1f} segundos")
        print(f"Vídeos encontrados: {stats['videos_found']}")
        print(f"Vídeos processados: {stats['videos_processed']}")
        print(f"Áudios baixados: {stats['audio_successful']}")
        print(f"Legendas baixadas: {stats['subtitle_successful']}")
        print(f"Pares completos (áudio+legenda): {stats['complete_pairs']}")
        
        # Taxa de sucesso
        if stats['videos_processed'] > 0:
            audio_rate = (stats['audio_successful'] / stats['videos_processed']) * 100
            print(f"Taxa sucesso áudio: {audio_rate:.1f}%")
            
            if stats['audio_successful'] > 0:
                subtitle_rate = (stats['subtitle_successful'] / stats['audio_successful']) * 100
                print(f"Taxa sucesso legendas: {subtitle_rate:.1f}%")
        
        print("="*60)
        
        # Mostra arquivos importantes criados
        print("\nARQUIVOS CRIADOS:")
        print(f"- Lista vídeos: {self.config.youtube_videos_file}")
        print(f"- Relatório: {self.config.output_dir}/execution_report.json")
        
        if stats['complete_pairs'] > 0:
            print(f"\nPARES ÁUDIO+LEGENDA PRONTOS:")
            for log_entry in self.success_log:
                if log_entry.get('has_audio') and log_entry.get('has_subtitle'):
                    video_id = log_entry['video_id']
                    audio_path = self.config.get_audio_file_path(video_id)
                    subtitle_path = self.config.get_subtitle_file_path(video_id)
                    print(f"- {video_id}: {audio_path} + {subtitle_path}")


class StreamlitDownloadManager:
    """
    Versão simplificada para integração direta com Streamlit
    Interface otimizada para componentes de interface gráfica
    """
    
    @staticmethod
    def execute_download(url: str, audio_quality: int = 256, api_key: Optional[str] = None) -> Dict:
        """
        Execução simplificada para Streamlit
        
        Args:
            url: URL do YouTube
            audio_quality: Qualidade do áudio (192, 256, 320)
            api_key: Chave API YouTube (opcional)
            
        Returns:
            Dict: Relatório da execução
        """
        try:
            # Cria configuração
            config = DownloadConfig(url=url, audio_quality=audio_quality, api_key=api_key)
            
            # Executa pipeline
            manager = DownloadManager(config)
            return manager.execute_full_pipeline()
            
        except Exception as e:
            return {
                'status': f'Erro na configuração: {e}',
                'stats': {'complete_pairs': 0},
                'error_log': [{'error': str(e)}]
            }
    
    @staticmethod
    def get_download_progress(output_dir: str) -> Dict:
        """
        Monitora progresso de download em tempo real (para Streamlit)
        
        Args:
            output_dir: Pasta de download para monitorar
            
        Returns:
            Dict: Status atual do download
        """
        try:
            base_path = Path(output_dir)
            
            if not base_path.exists():
                return {'status': 'not_started', 'progress': 0}
            
            # Conta arquivos de áudio e legenda
            audio_files = list(base_path.glob("**/*.mp3"))
            subtitle_files = list(base_path.glob("**/*.srt"))
            
            return {
                'status': 'in_progress',
                'audio_count': len(audio_files),
                'subtitle_count': len(subtitle_files),
                'complete_pairs': min(len(audio_files), len(subtitle_files)),
                'last_update': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {'status': 'error', 'error': str(e)}


# Função de conveniência para uso direto
def quick_download(url: str, quality: int = 256) -> Dict:
    """
    Download rápido para testes e uso simples
    
    Args:
        url: URL do YouTube
        quality: Qualidade do áudio
        
    Returns:
        Dict: Relatório da execução
    """
    return StreamlitDownloadManager.execute_download(url, quality)