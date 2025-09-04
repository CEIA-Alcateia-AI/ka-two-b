#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KATUBE PIPELINE PRINCIPAL - Orquestrador Completo
Executa pipeline completa de geracao de dataset TTS/STT a partir do YouTube
Baseado em configuracao centralizada - Filosofia KISS
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Adiciona src ao path para importacoes
src_path = os.path.join(os.path.dirname(__file__), 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Importacoes dos modulos da pipeline
from config import default_config, validate_and_show_config


def print_header():
    """Imprime cabecalho da pipeline"""
    print("=" * 70)
    print("KATUBE PIPELINE - GERACAO AUTOMATICA DE DATASET TTS/STT")
    print("Sistema Modular Integrado com Configuracao Centralizada")
    print("=" * 70)
    print(f"Inicio da execucao: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()


def print_step_header(step_number: int, step_name: str, enabled: bool):
    """Imprime cabecalho de cada etapa"""
    status = "EXECUTANDO" if enabled else "PULANDO"
    print(f"\n[ETAPA {step_number}/6] {step_name.upper()} - {status}")
    print("-" * 50)


def execute_download_step() -> dict:
    """Executa etapa de download de videos do YouTube"""
    try:
        from download.download_manager import DownloadManager
        
        print("Inicializando gerenciador de download...")
        manager = DownloadManager()
        
        print(f"URL configurada: {default_config.DOWNLOAD['target_url']}")
        print(f"Limite de videos: {default_config.DOWNLOAD['limit'] or 'Todos'}")
        
        result = manager.execute_download_pipeline()
        
        if result['status'] == 'success':
            print(f"Download concluido com sucesso!")
            print(f"Videos baixados: {result['stats']['videos_successful']}")
            print(f"Arquivos de audio: {result['stats']['audio_files_created']}")
            return {"success": True, "stats": result['stats']}
        else:
            print(f"Erro no download: {result['message']}")
            return {"success": False, "error": result['message']}
            
    except Exception as e:
        print(f"Erro inesperado no download: {e}")
        return {"success": False, "error": str(e)}


def execute_segmentation_step() -> dict:
    """Executa etapa de segmentacao de audio usando VAD"""
    try:
        from processing.audio_segmenter import segment_from_downloads_folder
        
        print("Iniciando segmentacao de audio com Silero VAD...")
        print(f"Configuracao: {default_config.SEGMENTATION['min_duration_sec']}-{default_config.SEGMENTATION['max_duration_sec']}s")
        
        result = segment_from_downloads_folder(
            downloads_path="downloads",
            overwrite=default_config.DOWNLOAD['overwrite_existing']
        )
        
        if result.get('batch_completed'):
            print(f"Segmentacao concluida!")
            print(f"Diretorios processados: {result['successful']}")
            print(f"Total de segmentos criados: {result['total_segments_created']}")
            return {"success": True, "segments_created": result['total_segments_created']}
        else:
            print(f"Erro na segmentacao: {result.get('error', 'Erro desconhecido')}")
            return {"success": False, "error": result.get('error')}
            
    except Exception as e:
        print(f"Erro inesperado na segmentacao: {e}")
        return {"success": False, "error": str(e)}


def execute_transcription_step() -> dict:
    """Executa transcricao com ambos os modelos (freds0 e lgris)"""
    transcription_results = {"freds0": None, "lgris": None}
    
    # Transcricao freds0 (Whisper)
    try:
        from transcription.freds0_transcriber import batch_transcribe_all_freds0
        
        print("\nExecutando transcricao freds0 (Whisper)...")
        print(f"Modelo: {default_config.TRANSCRIPTION_FREDS0['model_name']}")
        
        result = batch_transcribe_all_freds0(
            downloads_path="downloads",
            overwrite=default_config.TRANSCRIPTION_FREDS0['overwrite_existing']
        )
        
        if result['status'] == 'completed':
            summary = result['batch_summary']
            print(f"Freds0 concluido: {summary['successful_directories']} diretorios, {summary['total_segments_processed']} segmentos")
            transcription_results["freds0"] = {"success": True, "stats": summary}
        else:
            print(f"Erro freds0: {result.get('error')}")
            transcription_results["freds0"] = {"success": False, "error": result.get('error')}
            
    except Exception as e:
        print(f"Erro inesperado freds0: {e}")
        transcription_results["freds0"] = {"success": False, "error": str(e)}
    
    # Transcricao lgris (Wav2Vec2)
    try:
        from transcription.lgris_transcriber import batch_transcribe_all_lgris
        
        print("\nExecutando transcricao lgris (Wav2Vec2)...")
        print(f"Modelo: {default_config.TRANSCRIPTION_LGRIS['model_name']}")
        
        result = batch_transcribe_all_lgris(
            downloads_path="downloads",
            overwrite=default_config.TRANSCRIPTION_LGRIS['overwrite_existing']
        )
        
        if result['status'] == 'completed':
            summary = result['batch_summary']
            print(f"Lgris concluido: {summary['successful_directories']} diretorios, {summary['total_segments_processed']} segmentos")
            transcription_results["lgris"] = {"success": True, "stats": summary}
        else:
            print(f"Erro lgris: {result.get('error')}")
            transcription_results["lgris"] = {"success": False, "error": result.get('error')}
            
    except Exception as e:
        print(f"Erro inesperado lgris: {e}")
        transcription_results["lgris"] = {"success": False, "error": str(e)}
    
    # Avaliacao do resultado conjunto
    freds0_ok = transcription_results["freds0"] and transcription_results["freds0"]["success"]
    lgris_ok = transcription_results["lgris"] and transcription_results["lgris"]["success"]
    
    if freds0_ok and lgris_ok:
        return {"success": True, "results": transcription_results}
    elif freds0_ok or lgris_ok:
        print("Aviso: Apenas um modelo de transcricao funcionou, mas continuando...")
        return {"success": True, "results": transcription_results, "partial": True}
    else:
        print("Erro: Ambos os modelos de transcricao falharam")
        return {"success": False, "results": transcription_results}


def execute_normalization_step() -> dict:
    """Executa normalizacao de transcricoes (se habilitada)"""
    try:
        from processing.transcription_normalizer import batch_process_all
        
        print("Executando normalizacao de transcricoes...")
        print("Preparando textos para validacao cruzada...")
        
        # Chama funcao de normalizacao em lote
        batch_process_all("downloads")
        
        print("Normalizacao concluida!")
        return {"success": True}
        
    except Exception as e:
        print(f"Erro na normalizacao: {e}")
        return {"success": False, "error": str(e)}


def execute_validation_step() -> dict:
    """Executa validacao cruzada com threshold configurado"""
    try:
        from processing.transcription_validator import batch_validate_all
        
        print("Executando validacao cruzada...")
        print(f"Threshold configurado: {default_config.VALIDATION['similarity_threshold']:.1%}")
        
        # Executa validacao que ja salva em output/
        batch_validate_all("downloads")
        
        # Verifica se dataset final foi criado
        final_dataset = Path("output/final_dataset.csv")
        if final_dataset.exists():
            with open(final_dataset, 'r') as f:
                lines = f.readlines()
            approved_count = len(lines) - 1  # Remove header
            print(f"Dataset final criado: {approved_count} pares aprovados")
            return {"success": True, "approved_pairs": approved_count}
        else:
            print("Aviso: Dataset final nao foi criado")
            return {"success": False, "error": "Dataset final nao gerado"}
            
    except Exception as e:
        print(f"Erro na validacao: {e}")
        return {"success": False, "error": str(e)}


def execute_cleanup_step() -> dict:
    """Executa limpeza de arquivos intermediarios (se habilitada)"""
    try:
        from processing.cleanup_manager import run_cleanup_process
        
        print("Executando limpeza de arquivos intermediarios...")
        print("ATENCAO: Esta operacao remove permanentemente arquivos!")
        
        success = run_cleanup_process()
        
        if success:
            print("Limpeza concluida com sucesso!")
            return {"success": True}
        else:
            print("Limpeza nao foi executada devido a erros")
            return {"success": False, "error": "Falha na execucao da limpeza"}
            
    except Exception as e:
        print(f"Erro na limpeza: {e}")
        return {"success": False, "error": str(e)}


def print_final_summary(pipeline_results: dict, total_time: float):
    """Imprime resumo final da execucao completa"""
    print("\n" + "=" * 70)
    print("RELATORIO FINAL DA PIPELINE KATUBE")
    print("=" * 70)
    print(f"Tempo total de execucao: {total_time:.1f} segundos ({total_time/60:.1f} minutos)")
    print(f"Finalizacao: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Conta sucessos e falhas
    steps_executed = 0
    steps_successful = 0
    steps_failed = 0
    steps_skipped = 0
    
    step_names = ["download", "segmentation", "transcription", "normalization", "validation", "cleanup"]
    
    for step in step_names:
        if step in pipeline_results:
            steps_executed += 1
            if pipeline_results[step]["success"]:
                steps_successful += 1
            else:
                steps_failed += 1
        else:
            steps_skipped += 1
    
    print("RESUMO POR ETAPA:")
    print(f"  Etapas executadas: {steps_executed}")
    print(f"  Sucessos: {steps_successful}")
    print(f"  Falhas: {steps_failed}")
    print(f"  Puladas: {steps_skipped}")
    print()
    
    # Detalhes por etapa
    for step, result in pipeline_results.items():
        status_icon = "✅" if result["success"] else "❌"
        print(f"  {status_icon} {step.upper()}: {'SUCESSO' if result['success'] else 'FALHA'}")
        
        if not result["success"] and "error" in result:
            print(f"       Erro: {result['error']}")
        
        # Estatisticas especificas por etapa
        if step == "download" and result["success"]:
            print(f"       Videos baixados: {result.get('stats', {}).get('videos_successful', 'N/A')}")
        elif step == "segmentation" and result["success"]:
            print(f"       Segmentos criados: {result.get('segments_created', 'N/A')}")
        elif step == "validation" and result["success"]:
            print(f"       Pares aprovados: {result.get('approved_pairs', 'N/A')}")
    
    print()
    
    # Verifica resultado final
    validation_ok = pipeline_results.get("validation", {}).get("success", False)
    
    if validation_ok:
        print("🎉 PIPELINE CONCLUIDA COM SUCESSO!")
        print("📁 Dataset final disponivel em: output/final_dataset.csv")
        print("📁 Segmentos de audio em: output/segments/")
        print()
        print("PROXIMOS PASSOS:")
        print("  1. Verificar qualidade do dataset final")
        print("  2. Usar dataset para treinar modelos TTS/STT") 
        print("  3. Configurar pipeline para novos dados")
    else:
        print("⚠️  PIPELINE CONCLUIDA COM PROBLEMAS")
        print("Verifique os erros acima e ajuste configuracoes se necessario")
    
    print("=" * 70)


def main():
    """Funcao principal - Executa pipeline completa"""
    print_header()
    
    # Valida configuracoes antes de comecar
    print("Validando configuracoes...")
    if not validate_and_show_config():
        print("\n❌ Configuracoes invalidas! Corrija src/config.py antes de continuar.")
        return 1
    
    print("\n✅ Configuracoes validas! Iniciando pipeline...\n")
    
    # Controle de execucao
    pipeline_start = time.time()
    pipeline_results = {}
    
    # ETAPA 1: DOWNLOAD
    if default_config.PIPELINE_STEPS['download']:
        print_step_header(1, "DOWNLOAD", True)
        pipeline_results["download"] = execute_download_step()
    else:
        print_step_header(1, "DOWNLOAD", False)
        print("Etapa desabilitada no config.py")
    
    # ETAPA 2: SEGMENTACAO
    if default_config.PIPELINE_STEPS['segment']:
        print_step_header(2, "SEGMENTACAO", True)
        pipeline_results["segmentation"] = execute_segmentation_step()
    else:
        print_step_header(2, "SEGMENTACAO", False)
        print("Etapa desabilitada no config.py")
    
    # ETAPA 3: TRANSCRICAO
    if default_config.PIPELINE_STEPS['transcribe']:
        print_step_header(3, "TRANSCRICAO", True)
        pipeline_results["transcription"] = execute_transcription_step()
    else:
        print_step_header(3, "TRANSCRICAO", False)
        print("Etapa desabilitada no config.py")
    
    # ETAPA 4: NORMALIZACAO (Opcional)
    if default_config.PIPELINE_STEPS['normalize'] and default_config.NORMALIZATION['enabled']:
        print_step_header(4, "NORMALIZACAO", True)
        pipeline_results["normalization"] = execute_normalization_step()
    else:
        print_step_header(4, "NORMALIZACAO", False)
        reason = "desabilitada no config.py" if not default_config.PIPELINE_STEPS['normalize'] else "NORMALIZATION['enabled'] = False"
        print(f"Etapa {reason}")
    
    # ETAPA 5: VALIDACAO
    if default_config.PIPELINE_STEPS['validate']:
        print_step_header(5, "VALIDACAO", True)
        pipeline_results["validation"] = execute_validation_step()
    else:
        print_step_header(5, "VALIDACAO", False)
        print("Etapa desabilitada no config.py")
    
    # ETAPA 6: CLEANUP (Opcional)
    if default_config.PIPELINE_STEPS['cleanup'] and default_config.CLEANUP['enabled']:
        print_step_header(6, "CLEANUP", True)
        pipeline_results["cleanup"] = execute_cleanup_step()
    else:
        print_step_header(6, "CLEANUP", False)
        reason = "desabilitada no config.py" if not default_config.PIPELINE_STEPS['cleanup'] else "CLEANUP['enabled'] = False"
        print(f"Etapa {reason}")
    
    # Resumo final
    total_time = time.time() - pipeline_start
    print_final_summary(pipeline_results, total_time)
    
    # Codigo de saida baseado no sucesso da validacao
    validation_success = pipeline_results.get("validation", {}).get("success", False)
    return 0 if validation_success else 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️ Pipeline interrompida pelo usuario (Ctrl+C)")
        print("Dados parciais podem estar disponiveis em downloads/ e output/")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Erro critico na pipeline: {e}")
        print("Verifique configuracoes e dependencias")
        sys.exit(1)