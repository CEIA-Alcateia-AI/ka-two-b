#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Gerenciador de limpeza do pipeline Katube
# Consolida historicos e remove arquivos intermediarios apos processamento completo
# Versao KISS - simples e funcional
#

import os
import json
import shutil
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIGURAÇÃO DO USUÁRIO - EDITE AQUI
# =============================================================================

# ATENÇÃO: Esta funcionalidade REMOVE PERMANENTEMENTE arquivos intermediários
# Só execute após ter certeza de que o pipeline foi concluído com sucesso
# Os dados importantes ficam salvos na pasta 'output/', mas arquivos intermediários
# como .mp3, .wav, JSONs de transcrição, etc. serão DELETADOS

CLEANUP_ENABLED = False  # True = executa limpeza, False = só simula
                        # CUIDADO: True remove arquivos permanentemente!
                        
AUTO_CLEANUP_AFTER_VALIDATION = False  # True = limpa automaticamente após validação
                                      # False = executa apenas quando chamado manualmente

KEEP_EXECUTION_REPORTS = True  # True = mantém execution_report.json de cada playlist
                              # False = remove tudo, mantém só consolidado

# =============================================================================

def find_project_root():
    """
    Encontra a pasta raiz do projeto procurando pela pasta downloads
    """
    current_dir = os.path.abspath(os.getcwd())
    
    for _ in range(5):  # Máximo 5 níveis
        if os.path.exists(os.path.join(current_dir, 'downloads')):
            return current_dir
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            break
        current_dir = parent_dir
    
    return os.path.abspath('.')

def check_pipeline_completed():
    """
    Verifica se o pipeline foi completamente executado
    Critério: existe final_dataset.csv no output/ e tem registros
    """
    project_root = find_project_root()
    final_dataset = os.path.join(project_root, 'output', 'final_dataset.csv')
    
    if not os.path.exists(final_dataset):
        return False, "final_dataset.csv não encontrado na pasta output"
    
    # Verifica se o arquivo não está vazio
    try:
        with open(final_dataset, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if len(lines) <= 1:  # Só header
            return False, "final_dataset.csv existe mas está vazio"
        
        return True, f"Pipeline completo - {len(lines)-1} registros no dataset final"
        
    except Exception as e:
        return False, f"Erro ao verificar final_dataset.csv: {e}"

def find_all_execution_reports(downloads_base="downloads"):
    """
    Encontra todos os execution_report.json na estrutura de downloads
    """
    downloads_path = Path(downloads_base)
    
    if not downloads_path.exists():
        print(f"Diretório downloads não encontrado: {downloads_base}")
        return []
    
    execution_reports = []
    for report_file in downloads_path.rglob("execution_report.json"):
        try:
            with open(report_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            execution_reports.append({
                'file_path': str(report_file),
                'data': data,
                'directory': str(report_file.parent)
            })
            
        except Exception as e:
            print(f"Erro ao ler {report_file}: {e}")
    
    print(f"Encontrados {len(execution_reports)} execution_report.json")
    return execution_reports

def create_video_history_file(execution_reports, output_dir):
    """
    Cria arquivo incremental com URLs de todos os vídeos processados
    Evita duplicatas e mantém histórico atualizado
    
    Args:
        execution_reports (list): Lista de dados dos execution_report.json
        output_dir (str): Diretório output onde salvar o arquivo
        
    Returns:
        bool: True se processou com sucesso
    """
    history_file = os.path.join(output_dir, 'processed_videos_history.txt')
    
    # Carrega histórico existente se houver
    existing_urls = set()
    if os.path.exists(history_file):
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                existing_urls = set(line.strip() for line in f if line.strip())
            print(f"Histórico existente carregado: {len(existing_urls)} URLs")
        except Exception as e:
            print(f"Erro ao carregar histórico existente: {e}")
    
    # Coleta URLs de todos os execution reports
    all_new_urls = []
    duplicates_found = 0
    processing_stats = {
        'reports_processed': 0,
        'urls_found': 0,
        'successful_videos': 0,
        'urls_added': 0
    }
    
    for report_entry in execution_reports:
        processing_stats['reports_processed'] += 1
        data = report_entry['data']
        
        try:
            # Busca o arquivo youtube_videos.txt referenciado no execution_report
            arquivos_controle = data.get('config_summary', {}).get('arquivos_controle', {})
            lista_videos_path = arquivos_controle.get('lista_videos')
            
            # Converte path relativo para absoluto usando o diretório do execution_report
            if lista_videos_path and not os.path.isabs(lista_videos_path):
                # Constrói path absoluto baseado no diretório do execution_report
                report_dir = Path(report_entry['directory'])
                lista_videos_path = str(report_dir / Path(lista_videos_path).name)
            
            if not lista_videos_path or not os.path.exists(lista_videos_path):
                print(f"Arquivo youtube_videos.txt não encontrado: {lista_videos_path}")
                continue
            
            # Carrega URLs do arquivo .txt
            with open(lista_videos_path, 'r', encoding='utf-8') as f:
                playlist_urls = [line.strip() for line in f if line.strip()]
            
            processing_stats['urls_found'] += len(playlist_urls)
            
            # Extrai lista de vídeos processados com sucesso
            success_log = data.get('success_log', [])
            successful_video_ids = {entry.get('video_id') for entry in success_log if entry.get('video_id')}
            
            processing_stats['successful_videos'] += len(successful_video_ids)
            
            # Filtra URLs apenas dos vídeos processados com sucesso
            for url in playlist_urls:
                # Extrai video_id da URL para comparar com success_log
                if 'watch?v=' in url:
                    video_id = url.split('watch?v=')[1].split('&')[0]
                    
                    # Só inclui se o vídeo foi processado com sucesso
                    if video_id in successful_video_ids:
                        if url in existing_urls:
                            duplicates_found += 1
                            print(f"Duplicata encontrada: {url}")
                        else:
                            all_new_urls.append(url)
                            existing_urls.add(url)  # Atualiza set para próximas verificações
                            processing_stats['urls_added'] += 1
            
        except Exception as e:
            print(f"Erro ao processar report {report_entry['file_path']}: {e}")
            continue
    
    # Salva URLs novas no arquivo (modo append)
    if all_new_urls:
        try:
            with open(history_file, 'a', encoding='utf-8') as f:
                for url in all_new_urls:
                    f.write(url + '\n')
            
            print(f"Histórico de vídeos atualizado: {history_file}")
            print(f"URLs adicionadas: {len(all_new_urls)}")
            
        except Exception as e:
            print(f"Erro ao salvar histórico de vídeos: {e}")
            return False
    else:
        print("Nenhuma URL nova encontrada para adicionar ao histórico")
    
    # Exibe estatísticas do processamento
    print(f"ESTATÍSTICAS DO HISTÓRICO DE VÍDEOS:")
    print(f"  Reports processados: {processing_stats['reports_processed']}")
    print(f"  URLs encontradas nos arquivos: {processing_stats['urls_found']}")
    print(f"  Vídeos processados com sucesso: {processing_stats['successful_videos']}")
    print(f"  URLs novas adicionadas: {processing_stats['urls_added']}")
    print(f"  Duplicatas encontradas: {duplicates_found}")
    
    return True

def consolidate_download_history(execution_reports, output_dir):
    """
    Consolida todos os execution reports em um histórico único
    """
    consolidated_history = {
        "metadata": {
            "consolidation_date": datetime.now().isoformat(),
            "total_reports_processed": len(execution_reports),
            "katube_version": "pipeline_modular"
        },
        "processing_summary": {
            "total_playlists": 0,
            "total_videos_attempted": 0,
            "total_videos_successful": 0,
            "total_audio_files": 0,
            "total_subtitle_files": 0
        },
        "playlists": []
    }
    
    for report_entry in execution_reports:
        data = report_entry['data']
        
        # Extrai informações do config_summary
        config_summary = data.get('config_summary', {})
        stats = data.get('stats', {})
        
        playlist_info = {
            "playlist_id": config_summary.get('content_id', 'unknown'),
            "url_original": config_summary.get('url_original', ''),
            "tipo_detectado": config_summary.get('tipo_detectado', ''),
            "processing_date": stats.get('started_at', ''),
            "execution_time": stats.get('total_execution_time', 0),
            "videos_attempted": stats.get('videos_attempted', 0),
            "videos_successful": stats.get('videos_successful', 0),
            "videos_failed": stats.get('videos_failed', 0),
            "videos_skipped": stats.get('videos_skipped', 0),
            "audio_files_created": stats.get('audio_files_created', 0),
            "subtitle_files_created": stats.get('subtitle_files_created', 0),
            "execution_report_source": report_entry['file_path']
        }
        
        consolidated_history["playlists"].append(playlist_info)
        
        # Atualiza totais
        summary = consolidated_history["processing_summary"]
        summary["total_playlists"] += 1
        summary["total_videos_attempted"] += playlist_info["videos_attempted"]
        summary["total_videos_successful"] += playlist_info["videos_successful"]
        summary["total_audio_files"] += playlist_info["audio_files_created"]
        summary["total_subtitle_files"] += playlist_info["subtitle_files_created"]
    
    # Salva histórico consolidado
    history_file = os.path.join(output_dir, 'download_history.json')
    try:
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(consolidated_history, f, indent=2, ensure_ascii=False)
        
        print(f"Histórico consolidado salvo: {history_file}")
        return True
        
    except Exception as e:
        print(f"Erro ao salvar histórico: {e}")
        return False

def create_processing_log(execution_reports, output_dir):
    """
    Cria log detalhado de processamento em formato texto
    """
    log_file = os.path.join(output_dir, 'processing_log.txt')
    
    try:
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("KATUBE PIPELINE - LOG DE PROCESSAMENTO\n")
            f.write("=" * 50 + "\n")
            f.write(f"Data de consolidação: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total de playlists processadas: {len(execution_reports)}\n\n")
            
            for i, report_entry in enumerate(execution_reports, 1):
                data = report_entry['data']
                config_summary = data.get('config_summary', {})
                stats = data.get('stats', {})
                
                f.write(f"[{i}] PLAYLIST: {config_summary.get('content_id', 'unknown')}\n")
                f.write(f"    URL: {config_summary.get('url_original', '')}\n")
                f.write(f"    Data: {stats.get('started_at', '')}\n")
                f.write(f"    Vídeos baixados: {stats.get('videos_successful', 0)}/{stats.get('videos_attempted', 0)}\n")
                f.write(f"    Arquivos de áudio: {stats.get('audio_files_created', 0)}\n")
                f.write(f"    Arquivos de legenda: {stats.get('subtitle_files_created', 0)}\n")
                f.write(f"    Tempo total: {stats.get('total_execution_time', 0):.1f}s\n")
                f.write(f"    Fonte: {report_entry['file_path']}\n\n")
            
            # Adiciona informações do dataset final
            f.write("RESULTADO FINAL:\n")
            f.write("-" * 20 + "\n")
            
            # Lê informações do final_dataset.csv se existir
            final_dataset = os.path.join(output_dir, 'final_dataset.csv')
            if os.path.exists(final_dataset):
                with open(final_dataset, 'r', encoding='utf-8') as df:
                    lines = df.readlines()
                    f.write(f"Total de segmentos aprovados: {len(lines)-1}\n")
            
            # Conta arquivos na pasta segments
            segments_dir = os.path.join(output_dir, 'segments')
            if os.path.exists(segments_dir):
                wav_files = len([f for f in os.listdir(segments_dir) if f.endswith('.wav')])
                f.write(f"Arquivos de áudio no dataset final: {wav_files}\n")
        
        print(f"Log detalhado salvo: {log_file}")
        return True
        
    except Exception as e:
        print(f"Erro ao criar log: {e}")
        return False

def cleanup_intermediate_files(downloads_base="downloads", keep_execution_reports=True):
    """
    Remove arquivos intermediários mas mantém dados essenciais
    ATENÇÃO: Esta função REMOVE permanentemente arquivos!
    """
    downloads_path = Path(downloads_base)
    
    if not downloads_path.exists():
        print(f"Diretório downloads não encontrado: {downloads_base}")
        return False
    
    removed_items = []
    preserved_items = []
    errors = []
    
    # Itera sobre todas as pastas de playlist
    for playlist_dir in downloads_path.iterdir():
        if not playlist_dir.is_dir():
            continue
        
        playlist_name = playlist_dir.name
        print(f"Processando playlist: {playlist_name}")
        
        # Preserva execution_report.json se configurado
        execution_report = playlist_dir / "execution_report.json"
        if keep_execution_reports and execution_report.exists():
            preserved_items.append(str(execution_report))
            # Move para pasta temporária
            temp_report = playlist_dir.parent / f"{playlist_name}_execution_report.json"
            shutil.move(str(execution_report), str(temp_report))
        
        # Remove toda a pasta da playlist
        try:
            shutil.rmtree(playlist_dir)
            removed_items.append(f"Playlist completa: {playlist_name}")
            
            # Restaura execution_report se necessário
            if keep_execution_reports:
                playlist_dir.mkdir()
                temp_report = playlist_dir.parent / f"{playlist_name}_execution_report.json"
                if temp_report.exists():
                    shutil.move(str(temp_report), str(execution_report))
                    preserved_items.append(str(execution_report))
            
        except Exception as e:
            errors.append(f"Erro ao remover {playlist_dir}: {e}")
    
    # Remove arquivos soltos na pasta downloads
    for item in downloads_path.iterdir():
        if item.is_file() and item.suffix in ['.txt', '.json']:
            try:
                item.unlink()
                removed_items.append(f"Arquivo: {item.name}")
            except Exception as e:
                errors.append(f"Erro ao remover {item}: {e}")
    
    return {
        "removed_count": len(removed_items),
        "preserved_count": len(preserved_items),
        "errors_count": len(errors),
        "removed_items": removed_items,
        "preserved_items": preserved_items,
        "errors": errors
    }

def run_cleanup_process():
    """
    Executa processo completo de limpeza
    Função principal do módulo
    """
    print("KATUBE CLEANUP MANAGER")
    print("=" * 50)
    print("ATENÇÃO: Esta operação pode REMOVER PERMANENTEMENTE arquivos intermediários!")
    print(f"Modo de limpeza: {'ATIVADO' if CLEANUP_ENABLED else 'SIMULAÇÃO'}")
    print("=" * 50)
    
    # Verifica se pipeline foi completamente executado
    completed, message = check_pipeline_completed()
    print(f"Status do pipeline: {message}")
    
    if not completed:
        print("AVISO: Pipeline não parece ter sido completado.")
        print("É recomendável executar a limpeza apenas após validação completa.")
        if not CLEANUP_ENABLED:
            print("Executando em modo simulação...")
        else:
            print("Continuando com limpeza... (cuidado!)")
    
    # Encontra raiz do projeto e configura output
    project_root = find_project_root()
    output_dir = os.path.join(project_root, 'output')
    downloads_dir = os.path.join(project_root, 'downloads')
    
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Projeto: {project_root}")
    print(f"Downloads: {downloads_dir}")
    print(f"Output: {output_dir}")
    
    # Encontra todos os execution reports
    print("\nBuscando execution reports...")
    execution_reports = find_all_execution_reports(downloads_dir)
    
    if not execution_reports:
        print("Nenhum execution report encontrado. Nada para processar.")
        return False
    
    # Consolida histórico
    print("\nConsolidando histórico de downloads...")
    history_saved = consolidate_download_history(execution_reports, output_dir)
    
    # Cria histórico incremental de vídeos
    print("Criando histórico incremental de vídeos...")
    video_history_saved = create_video_history_file(execution_reports, output_dir)
    
    # Cria log detalhado
    print("Criando log de processamento...")
    log_saved = create_processing_log(execution_reports, output_dir)
    
    if not (history_saved and log_saved and video_history_saved):
        print("Erro ao salvar dados de histórico. Abortando limpeza por segurança.")
        return False
    
    # Executa limpeza
    if CLEANUP_ENABLED:
        print("\nExecutando limpeza de arquivos intermediários...")
        print("ATENÇÃO: Removendo arquivos permanentemente!")
        
        cleanup_result = cleanup_intermediate_files(
            downloads_dir, 
            keep_execution_reports=KEEP_EXECUTION_REPORTS
        )
        
        print(f"Itens removidos: {cleanup_result['removed_count']}")
        print(f"Itens preservados: {cleanup_result['preserved_count']}")
        print(f"Erros: {cleanup_result['errors_count']}")
        
        if cleanup_result['errors']:
            print("Erros durante limpeza:")
            for error in cleanup_result['errors']:
                print(f"  - {error}")
        
        # Salva relatório de limpeza
        cleanup_report_file = os.path.join(output_dir, 'cleanup_report.json')
        with open(cleanup_report_file, 'w', encoding='utf-8') as f:
            cleanup_result['cleanup_date'] = datetime.now().isoformat()
            json.dump(cleanup_result, f, indent=2, ensure_ascii=False)
        
    else:
        print("\nMODO SIMULAÇÃO - Nenhum arquivo foi removido")
        print("Para executar limpeza real, altere CLEANUP_ENABLED = True")
        
        # Simula limpeza para mostrar o que seria removido
        print("\nItens que seriam removidos:")
        for playlist_dir in Path(downloads_dir).iterdir():
            if playlist_dir.is_dir():
                wav_count = len(list(playlist_dir.rglob("*.wav")))
                json_count = len(list(playlist_dir.rglob("*.json")))
                print(f"  - Playlist {playlist_dir.name}: ~{wav_count} WAV + {json_count} JSON")
    
    print("\n" + "=" * 50)
    print("PROCESSO DE LIMPEZA CONCLUÍDO")
    print(f"Dados consolidados salvos em: {output_dir}")
    print("=" * 50)
    
    return True

def main():
    """
    Função principal para execução standalone
    """
    try:
        success = run_cleanup_process()
        
        if success:
            print("\nLimpeza concluída com sucesso!")
        else:
            print("\nLimpeza não foi executada devido a erros.")
            
    except KeyboardInterrupt:
        print("\nOperação interrompida pelo usuário")
    except Exception as e:
        print(f"\nErro inesperado durante limpeza: {e}")

if __name__ == "__main__":
    main()