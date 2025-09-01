#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orquestrador principal para download YouTube usando yt-dlp
Executa downloads unificados de áudio + legendas com verificação de duplicatas
Preparado para integração com Streamlit
"""

import subprocess
import json
import time
import os
from datetime import datetime
from random import randint
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from download_config import DownloadConfig, create_config_instance


class DownloadManager:
    """
    Gerenciador principal de downloads YouTube
    Usa yt-dlp para download unificado de áudio e legendas
    """
    
    def __init__(self, config: Optional[DownloadConfig] = None):
        """
        Inicializa gerenciador com configuração
        
        Args:
            config: Configuração personalizada (usa padrão se None)
        """
        self.config = config or create_config_instance()
        
        # Estatísticas de execução
        self.stats = {
            'started_at': None,
            'finished_at': None,
            'total_execution_time': 0,
            'videos_attempted': 0,
            'videos_successful': 0,
            'videos_failed': 0,
            'videos_skipped': 0,
            'audio_files_created': 0,
            'subtitle_files_created': 0
        }
        
        # Logs detalhados
        self.success_log = []
        self.error_log = []
    
    def execute_download_pipeline(self) -> Dict:
        """
        Executa pipeline completo de download
        Método principal para uso standalone ou via Streamlit
        
        Returns:
            Dict: Relatório completo da execução
        """
        print("="*60)
        print("INICIANDO DOWNLOAD YOUTUBE - PIPELINE UNIFICADO")
        print("="*60)
        
        self.stats['started_at'] = datetime.now()
        
        try:
            # Valida configurações antes de começar
            validation = self.config.validate_config()
            if not validation['valid']:
                return self._create_error_report("Configurações inválidas", validation['errors'])
            
            # Exibe resumo da configuração
            self._print_config_summary()
            
            # Obtém lista de vídeos para download
            video_ids = self._get_video_list()
            
            if not video_ids:
                return self._create_error_report("Nenhum vídeo encontrado ou todos já baixados")
            
            # Executa downloads com delays
            successful_downloads = self._execute_downloads_with_delays(video_ids)
            
            # Organiza arquivos baixados
            self._organize_downloaded_files(successful_downloads)
            
            # Cria relatório final
            return self._create_success_report()
            
        except KeyboardInterrupt:
            print("\n⚠️ Download interrompido pelo usuário")
            return self._create_error_report("Interrompido pelo usuário")
            
        except Exception as e:
            print(f"\n❌ Erro inesperado: {e}")
            return self._create_error_report(f"Erro inesperado: {e}")
        
        finally:
            self.stats['finished_at'] = datetime.now()
            if self.stats['started_at']:
                elapsed = self.stats['finished_at'] - self.stats['started_at']
                self.stats['total_execution_time'] = elapsed.total_seconds()
    
    def _get_video_list(self) -> List[str]:
        """
        Obtém lista de video_ids para download
        Aplica verificação de duplicatas
        
        Returns:
            List[str]: Lista de video_ids novos para download
        """
        print("\n🔍 Extraindo lista de vídeos...")
        
        try:
            # Usa yt-dlp para extrair lista sem baixar
            extract_cmd = [
                "yt-dlp",
                "--flat-playlist",    # Só extrai URLs, não baixa
                "--print", "id",      # Imprime apenas IDs
                "--quiet",            # Sem logs extras
                self.config.target_url
            ]
            
            # Adiciona limite se configurado
            if self.config.DOWNLOAD_LIMIT > 0:
                extract_cmd.extend(["--playlist-end", str(self.config.DOWNLOAD_LIMIT)])
            
            # Executa comando
            result = subprocess.run(extract_cmd, capture_output=True, text=True, check=True)
            
            # Processa IDs retornados
            all_video_ids = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            
            print(f"Total de vídeos encontrados: {len(all_video_ids)}")
            
            # Aplica verificação de duplicatas
            new_video_ids = []
            skipped_count = 0
            
            for video_id in all_video_ids:
                if self.config.is_video_downloaded(video_id):
                    skipped_count += 1
                    self.stats['videos_skipped'] += 1
                else:
                    new_video_ids.append(video_id)
            
            if skipped_count > 0:
                print(f"📋 Ignorando {skipped_count} vídeos já baixados")
            
            print(f"🆕 Vídeos novos para download: {len(new_video_ids)}")
            
            # Salva lista completa para controle
            self._save_videos_list(all_video_ids)
            
            return new_video_ids
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Erro ao extrair lista de vídeos: {e}")
            return []
        except Exception as e:
            print(f"❌ Erro inesperado na extração: {e}")
            return []
    
    def _execute_downloads_with_delays(self, video_ids: List[str]) -> List[str]:
        """
        Executa downloads com delays anti-bloqueio
        
        Args:
            video_ids: Lista de IDs para baixar
            
        Returns:
            List[str]: IDs dos downloads bem-sucedidos
        """
        successful_downloads = []
        
        print(f"\n⬇️ Iniciando download de {len(video_ids)} vídeos...")
        print(f"Delay configurado: {self.config.DELAY_MIN_SECONDS}-{self.config.DELAY_MAX_SECONDS}s")
        
        for i, video_id in enumerate(video_ids, 1):
            print(f"\n[{i}/{len(video_ids)}] Baixando: {video_id}")
            
            self.stats['videos_attempted'] += 1
            
            # Executa download individual
            success = self._download_single_video(video_id)
            
            if success:
                successful_downloads.append(video_id)
                self.stats['videos_successful'] += 1
                print(f"✅ Sucesso: {video_id}")
            else:
                self.stats['videos_failed'] += 1
                print(f"❌ Falha: {video_id}")
            
            # Delay anti-bloqueio (exceto no último)
            if i < len(video_ids):
                delay = randint(self.config.DELAY_MIN_SECONDS, self.config.DELAY_MAX_SECONDS)
                print(f"⏳ Aguardando {delay}s (anti-bloqueio)...")
                time.sleep(delay)
        
        return successful_downloads
    
    def _download_single_video(self, video_id: str) -> bool:
        """
        Baixa áudio e legenda de um vídeo usando yt-dlp
        
        Args:
            video_id: ID único do vídeo
            
        Returns:
            bool: True se download bem-sucedido
        """
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        
        try:
            # Constrói comando yt-dlp específico para este vídeo
            cmd_args = [
                "yt-dlp",
                
                # Extração de áudio
                "-x",
                "--audio-format", self.config.AUDIO_FORMAT,
                "--audio-quality", str(self.config.AUDIO_QUALITY),
                
                # Download de legendas
                "--write-subs",
                "--write-auto-subs", 
                "--sub-lang", ",".join(self.config.SUBTITLE_LANGUAGES),
                
                # Output específico para este vídeo
                "--output", str(self.config.get_video_output_path(video_id) / f"{video_id}.%(ext)s"),
                
                # Controle de erros
                "--ignore-errors",
                "--quiet",
                
                # URL do vídeo
                video_url
            ]
            
            # Executa download
            result = subprocess.run(cmd_args, capture_output=True, text=True, check=False)
            
            # Verifica se arquivos foram criados
            audio_file = self.config.get_audio_file_path(video_id)
            subtitle_file = self.config.get_subtitle_file_path(video_id)
            
            # Valida resultado
            audio_ok = audio_file.exists() and audio_file.stat().st_size > 1024
            subtitle_ok = subtitle_file.exists() and subtitle_file.stat().st_size > 0
            
            if audio_ok:
                self.stats['audio_files_created'] += 1
            
            if subtitle_ok:
                self.stats['subtitle_files_created'] += 1
            
            # Log do resultado
            download_result = {
                'video_id': video_id,
                'timestamp': datetime.now().isoformat(),
                'audio_downloaded': audio_ok,
                'subtitle_downloaded': subtitle_ok,
                'audio_size_mb': round(audio_file.stat().st_size / (1024*1024), 2) if audio_ok else 0,
                'subtitle_size_kb': round(subtitle_file.stat().st_size / 1024, 2) if subtitle_ok else 0
            }
            
            if audio_ok:  # Sucesso se pelo menos áudio foi baixado
                self.success_log.append(download_result)
                return True
            else:
                download_result['error'] = "Áudio não foi baixado"
                self.error_log.append(download_result)
                return False
                
        except Exception as e:
            error_entry = {
                'video_id': video_id,
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'command_used': ' '.join(cmd_args) if 'cmd_args' in locals() else 'N/A'
            }
            self.error_log.append(error_entry)
            return False
    
    def _organize_downloaded_files(self, video_ids: List[str]) -> None:
        """
        Organiza e valida arquivos baixados
        Remove arquivos temporários e renomeia se necessário
        
        Args:
            video_ids: Lista de IDs baixados
        """
        print(f"\n📁 Organizando {len(video_ids)} downloads...")
        
        for video_id in video_ids:
            video_dir = self.config.get_video_output_path(video_id)
            
            if not video_dir.exists():
                continue
            
            # Remove arquivos temporários comuns do yt-dlp
            temp_patterns = ["*.part", "*.ytdl", "*.tmp"]
            for pattern in temp_patterns:
                for temp_file in video_dir.glob(pattern):
                    try:
                        temp_file.unlink()
                        print(f"🧹 Removido arquivo temporário: {temp_file.name}")
                    except Exception as e:
                        print(f"⚠️ Erro ao remover {temp_file}: {e}")
            
            # Renomeia arquivos de legenda se necessário
            self._standardize_subtitle_file(video_id)
    
    def _standardize_subtitle_file(self, video_id: str) -> None:
        """
        Padroniza nome do arquivo de legenda
        yt-dlp pode criar nomes como video_id.pt-BR.srt
        
        Args:
            video_id: ID do vídeo para padronizar
        """
        video_dir = self.config.get_video_output_path(video_id)
        target_subtitle = self.config.get_subtitle_file_path(video_id)
        
        # Se já existe no formato correto, não faz nada
        if target_subtitle.exists():
            return
        
        # Busca arquivos de legenda com padrões do yt-dlp
        subtitle_patterns = [
            f"{video_id}.pt-BR.srt",
            f"{video_id}.pt.srt", 
            f"{video_id}.pt-BR.vtt",
            f"{video_id}.pt.vtt"
        ]
        
        for pattern in subtitle_patterns:
            source_file = video_dir / pattern
            if source_file.exists():
                try:
                    source_file.rename(target_subtitle)
                    print(f"📝 Legenda renomeada: {pattern} → {target_subtitle.name}")
                    break
                except Exception as e:
                    print(f"⚠️ Erro ao renomear legenda: {e}")
    
    def _save_videos_list(self, video_ids: List[str]) -> None:
        """
        Salva lista de vídeos no arquivo de controle
        Formato compatível com sistema original
        
        Args:
            video_ids: Lista de IDs para salvar
        """
        try:
            with open(self.config.videos_list_file, 'w', encoding='utf-8') as f:
                for video_id in video_ids:
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                    f.write(video_url + '\n')
            
            print(f"📝 Lista salva: {self.config.videos_list_file}")
            
        except Exception as e:
            print(f"⚠️ Erro ao salvar lista: {e}")
    
    def _print_config_summary(self) -> None:
        """Imprime resumo das configurações antes de iniciar"""
        summary = self.config.create_summary()
        
        print("📋 CONFIGURAÇÕES ATIVAS:")
        print(f"  URL: {summary['url_original']}")
        print(f"  Tipo: {summary['tipo_detectado']}")
        print(f"  Limite: {summary['limite_downloads']}")
        print(f"  Qualidade: {summary['qualidade_audio']}")
        print(f"  Formato: {summary['formato_audio']}")
        print(f"  Delays: {summary['delay_range']}")
        print(f"  Legendas: {summary['linguagens_legenda']}")
        print(f"  Output: {summary['pasta_saida']}")
    
    def _create_success_report(self) -> Dict:
        """Cria relatório de sucesso com estatísticas"""
        report = {
            'status': 'success',
            'message': 'Pipeline executado com sucesso',
            'config_summary': self.config.create_summary(),
            'stats': self.stats,
            'success_log': self.success_log,
            'error_log': self.error_log,
            'files_created': {
                'videos_list': str(self.config.videos_list_file),
                'success_log': str(self.config.success_log_file),
                'error_log': str(self.config.error_log_file)
            }
        }
        
        # Salva relatório em arquivo JSON
        self._save_execution_report(report)
        self._print_final_summary()
        
        return report
    
    def _create_error_report(self, error_message: str, details: Optional[List] = None) -> Dict:
        """Cria relatório de erro"""
        return {
            'status': 'error',
            'message': error_message,
            'details': details or [],
            'config_summary': self.config.create_summary(),
            'stats': self.stats,
            'error_log': self.error_log
        }
    
    def _save_execution_report(self, report: Dict) -> None:
        """
        Salva relatório de execução em arquivo JSON
        Útil para debugging e integração com Streamlit
        """
        try:
            report_file = self.config.output_dir / "execution_report.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"📊 Relatório salvo: {report_file}")
            
        except Exception as e:
            print(f"⚠️ Erro ao salvar relatório: {e}")
    
    def _print_final_summary(self) -> None:
        """Imprime resumo final da execução"""
        stats = self.stats
        
        print("\n" + "="*60)
        print("RELATÓRIO FINAL DE DOWNLOAD")
        print("="*60)
        print(f"Tempo total: {stats['total_execution_time']:.1f} segundos")
        print(f"Vídeos tentados: {stats['videos_attempted']}")
        print(f"Sucessos: {stats['videos_successful']}")
        print(f"Falhas: {stats['videos_failed']}")
        print(f"Já existiam: {stats['videos_skipped']}")
        print(f"Arquivos de áudio: {stats['audio_files_created']}")
        print(f"Arquivos de legenda: {stats['subtitle_files_created']}")
        
        # Taxa de sucesso
        if stats['videos_attempted'] > 0:
            success_rate = (stats['videos_successful'] / stats['videos_attempted']) * 100
            print(f"Taxa de sucesso: {success_rate:.1f}%")
        
        print("="*60)
        
        # Lista arquivos criados
        if stats['videos_successful'] > 0:
            print(f"\n📁 ARQUIVOS CRIADOS:")
            for entry in self.success_log:
                video_id = entry['video_id']
                audio_size = entry.get('audio_size_mb', 0)
                subtitle_size = entry.get('subtitle_size_kb', 0)
                
                audio_status = f"Audio: {audio_size}MB" if entry.get('audio_downloaded') else "Sem áudio"
                subtitle_status = f"Legenda: {subtitle_size}KB" if entry.get('subtitle_downloaded') else "Sem legenda"
                
                print(f"  {video_id}: {audio_status} | {subtitle_status}")


# ========================================
# FUNÇÕES DE CONVENIÊNCIA PARA USO EXTERNO
# ========================================

def quick_download(url: str, audio_quality: int = 256, limit: int = 5) -> Dict:
    """
    Download rápido para testes
    
    Args:
        url: URL do YouTube
        audio_quality: Qualidade em kbps
        limit: Limite de vídeos
        
    Returns:
        Dict: Relatório da execução
    """
    # Cria configuração temporária
    config = create_config_instance(url)
    config.AUDIO_QUALITY = audio_quality
    config.DOWNLOAD_LIMIT = limit
    
    # Executa download
    manager = DownloadManager(config)
    return manager.execute_download_pipeline()


def download_with_custom_config(config_dict: Dict) -> Dict:
    """
    Download com configurações personalizadas
    Útil para integração com Streamlit
    
    Args:
        config_dict: Dicionário com configurações customizadas
        
    Returns:
        Dict: Relatório da execução
    """
    # Cria configuração base
    config = create_config_instance(config_dict.get('url'))
    
    # Aplica customizações
    for key, value in config_dict.items():
        if hasattr(config, key.upper()):
            setattr(config, key.upper(), value)
    
    # Executa download
    manager = DownloadManager(config)
    return manager.execute_download_pipeline()


def check_download_status(output_dir: str) -> Dict:
    """
    Verifica status de downloads em andamento
    Útil para progress bars no Streamlit
    
    Args:
        output_dir: Diretório de downloads para verificar
        
    Returns:
        Dict: Status atual dos downloads
    """
    try:
        base_path = Path(output_dir)
        
        if not base_path.exists():
            return {'status': 'not_started', 'progress': 0}
        
        # Conta arquivos existentes
        audio_files = list(base_path.glob("**/*.mp3")) + list(base_path.glob("**/*.wav"))
        subtitle_files = list(base_path.glob("**/*.srt"))
        
        return {
            'status': 'completed' if audio_files else 'not_started',
            'audio_count': len(audio_files),
            'subtitle_count': len(subtitle_files),
            'complete_pairs': min(len(audio_files), len(subtitle_files)),
            'last_update': datetime.now().isoformat()
        }
        
    except Exception as e:
        return {'status': 'error', 'error': str(e)}


# ========================================
# EXECUÇÃO STANDALONE
# ========================================

def main():
    """
    Função principal para execução standalone
    Usa configurações padrão do arquivo de config
    """
    print("🎵 KATUBE DOWNLOAD MANAGER - Versão Simplificada")
    print("Gerando dataset de áudio para TTS/STT")
    
    # Cria gerenciador com configurações padrão
    manager = DownloadManager()
    
    # Executa pipeline completo
    report = manager.execute_download_pipeline()
    
    # Exibe resultado final
    if report['status'] == 'success':
        print("\n🎉 Pipeline concluído com sucesso!")
        print(f"Pasta de downloads: {manager.config.output_dir}")
    else:
        print(f"\n❌ Pipeline falhou: {report['message']}")
    
    return report


if __name__ == "__main__":
    # Execução direta via terminal
    try:
        result = main()
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n💥 Erro fatal: {e}")