#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Validador de transcrições usando distância Levenshtein
# Versão KISS - simples e funcional
# Baseado no projeto Katube original (validation.txt)
#

import json
import os
import csv
import shutil
from datetime import datetime
from textdistance import levenshtein

# =============================================================================
# CONFIGURAÇÃO VIA CONFIG.PY - Configuração centralizada
# =============================================================================

# Importa configurações do sistema centralizado
try:
    import sys
    import os
    # Adiciona o diretório src ao path de forma robusta
    src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)))
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    from config import default_config
    
    # Threshold de similaridade vindo do config.py centralizado
    SIMILARITY_THRESHOLD = default_config.VALIDATION['similarity_threshold']
    
    print(f"Threshold de validação carregado do config.py: {SIMILARITY_THRESHOLD:.1%}")
    
except ImportError as e:
    print(f"Aviso: Não foi possível importar config.py, usando threshold padrão: {e}")
    # Fallback para valor padrão se config.py não estiver disponível
    SIMILARITY_THRESHOLD = 0.7  # Valor conservador por segurança

# =============================================================================
def find_project_root():
    """
    Encontra a pasta raiz do projeto procurando pela pasta downloads
    """
    current_dir = os.path.abspath(os.getcwd())
    
    # Sobe até encontrar a pasta downloads ou chegar na raiz
    for _ in range(5):  # Máximo 5 níveis
        if os.path.exists(os.path.join(current_dir, 'downloads')):
            return current_dir
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:  # Chegou na raiz do sistema
            break
        current_dir = parent_dir
    
    # Se não encontrar, usa diretório atual
    return os.path.abspath('.')

def setup_output_directory():
    """
    Cria estrutura da pasta output
    """
    project_root = find_project_root()
    output_dir = os.path.join(project_root, 'output')
    segments_dir = os.path.join(output_dir, 'segments')
    
    # Cria diretórios se não existirem
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(segments_dir, exist_ok=True)
    
    return output_dir, segments_dir

def calculate_similarity(text1, text2):
    """
    Calcula similaridade Levenshtein entre dois textos
    Baseado em validation.txt
    """
    if not text1 or not text2:
        return 0.0
    
    # Remove espaços extras para comparação mais justa
    clean_text1 = text1.strip()
    clean_text2 = text2.strip()
    
    if not clean_text1 or not clean_text2:
        return 0.0
    
    # Calcula similaridade normalizada (0.0 a 1.0)
    similarity = levenshtein.normalized_similarity(clean_text1, clean_text2)
    return similarity

def process_validation(segments_path, output_segments_dir):
    """
    Processa validação de uma pasta de segments
    Retorna lista de arquivos aprovados para consolidação
    """
    print(f"Validando: {segments_path}")
    
    # Arquivos de entrada e saída
    input_file = os.path.join(segments_path, "normalized_transcriptions.json")
    results_file = os.path.join(segments_path, "validation_results.json")
    csv_file = os.path.join(segments_path, "approved_dataset.csv")
    
    # Verifica se arquivo de entrada existe
    if not os.path.exists(input_file):
        print(f"Arquivo não encontrado: {input_file}")
        return []
    
    # Carrega dados normalizados
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Erro ao carregar {input_file}: {e}")
        return []
    
    normalized_pairs = data.get('normalized_pairs', {})
    if not normalized_pairs:
        print("Nenhum par normalizado encontrado")
        return []
    
    print(f"Avaliando {len(normalized_pairs)} pares com threshold {SIMILARITY_THRESHOLD}")
    
    # Estatísticas
    total_pairs = 0
    approved_pairs = 0
    rejected_pairs = 0
    invalid_pairs = 0
    
    # Lista para CSV final
    approved_data = []
    
    # Processa cada par
    for segment_id, pair_data in normalized_pairs.items():
        total_pairs += 1
        
        # Pega textos normalizados para comparação
        lgris_normalized = pair_data.get('lgris_normalized')
        freds0_normalized = pair_data.get('freds0_normalized')
        
        # Pega textos originais para salvar no dataset
        lgris_original = pair_data.get('lgris_original')
        freds0_original = pair_data.get('freds0_original')
        
        # Verifica se textos são válidos
        if not lgris_normalized or not freds0_normalized:
            invalid_pairs += 1
            continue
        
        # Calcula similaridade
        similarity = calculate_similarity(lgris_normalized, freds0_normalized)
        
        # Verifica se passa no threshold
        if similarity >= SIMILARITY_THRESHOLD:
            approved_pairs += 1
            
            # Adiciona textos ORIGINAIS ao dataset aprovado
            approved_entry = {
                'filename': f"{segment_id}.wav",
                'lgris_text': lgris_original or '',
                'freds0_text': freds0_original or '',
                'similarity': similarity
            }
            approved_data.append(approved_entry)
            
            # Copia arquivo de áudio aprovado para pasta output
            source_audio = os.path.join(segments_path, f"{segment_id}.wav")
            if os.path.exists(source_audio):
                try:
                    dest_audio = os.path.join(output_segments_dir, f"{segment_id}.wav")
                    shutil.copy2(source_audio, dest_audio)
                except Exception as e:
                    print(f"Erro ao copiar {source_audio}: {e}")
        else:
            rejected_pairs += 1
    
    # Salva CSV local com textos originais aprovados
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['filename', 'lgris_text', 'freds0_text', 'similarity']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for row in approved_data:
                writer.writerow(row)
        
        print(f"Dataset local salvo: {csv_file}")
        
    except Exception as e:
        print(f"Erro ao salvar CSV: {e}")
        return []
    
    # Cria relatório de validação
    validation_results = {
        "metadata": {
            "processing_date": datetime.now().isoformat(),
            "similarity_threshold": SIMILARITY_THRESHOLD,
            "total_pairs": total_pairs,
            "approved_pairs": approved_pairs,
            "rejected_pairs": rejected_pairs,
            "invalid_pairs": invalid_pairs,
            "approval_rate": round((approved_pairs / total_pairs) * 100, 2) if total_pairs > 0 else 0
        },
        "statistics": {
            "segments_folder": os.path.basename(segments_path),
            "input_file": "normalized_transcriptions.json",
            "output_csv": "approved_dataset.csv"
        }
    }
    
    # Salva relatório JSON
    try:
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(validation_results, f, indent=2, ensure_ascii=False)
        
        print(f"Relatório salvo: {results_file}")
        
    except Exception as e:
        print(f"Erro ao salvar relatório: {e}")
        return []
    
    # Exibe estatísticas
    print("=" * 40)
    print(f"RESULTADOS DA VALIDAÇÃO:")
    print(f"  Total de pares: {total_pairs}")
    print(f"  Aprovados: {approved_pairs}")
    print(f"  Rejeitados: {rejected_pairs}")
    print(f"  Inválidos: {invalid_pairs}")
    print(f"  Taxa de aprovação: {validation_results['metadata']['approval_rate']}%")
    print(f"  Arquivos de áudio copiados: {approved_pairs}")
    
    return approved_data

def consolidate_datasets(all_approved_data, output_dir):
    """
    Consolida todos os dados aprovados em um CSV único
    """
    if not all_approved_data:
        print("Nenhum dado aprovado para consolidar")
        return
    
    final_csv = os.path.join(output_dir, 'final_dataset.csv')
    
    try:
        with open(final_csv, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['filename', 'lgris_text', 'freds0_text', 'similarity']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for row in all_approved_data:
                writer.writerow(row)
        
        print(f"Dataset final consolidado salvo: {final_csv}")
        print(f"Total de registros: {len(all_approved_data)}")
        
    except Exception as e:
        print(f"Erro ao salvar dataset final: {e}")

def batch_validate_all(base_dir):
    """
    Valida todos os diretórios de segments e consolida resultados
    """
    print(f"Procurando arquivos normalized_transcriptions.json em: {base_dir}")
    print(f"Threshold configurado: {SIMILARITY_THRESHOLD}")
    print("=" * 60)
    
    # Configura pasta output
    output_dir, output_segments_dir = setup_output_directory()
    print(f"Pasta output: {output_dir}")
    print(f"Pasta segments: {output_segments_dir}")
    print("=" * 60)
    
    processed = 0
    all_approved_data = []  # Lista consolidada de todos os dados aprovados
    
    for root, dirs, files in os.walk(base_dir):
        if 'normalized_transcriptions.json' in files:
            approved_data = process_validation(root, output_segments_dir)
            if approved_data:
                processed += 1
                all_approved_data.extend(approved_data)  # Adiciona à lista consolidada
            
            print("-" * 60)
    
    # Consolida todos os dados em um CSV único
    consolidate_datasets(all_approved_data, output_dir)
    
    print(f"RESUMO GERAL:")
    print(f"  Diretórios processados: {processed}")
    print(f"  Total de pares aprovados: {len(all_approved_data)}")
    print(f"  Threshold usado: {SIMILARITY_THRESHOLD}")
    print(f"  Arquivos de áudio copiados: {len(all_approved_data)}")
    print(f"  Dataset final: {os.path.join(output_dir, 'final_dataset.csv')}")
    print(f"  Pasta de áudios: {output_segments_dir}")

def main():
    """
    Função principal
    """
    print("KATUBE TRANSCRIPTION VALIDATOR - Versão KISS")
    print("Validação cruzada usando distância Levenshtein")
    print("=" * 60)
    print(f"Threshold configurado: {SIMILARITY_THRESHOLD}")
    print("(Para alterar, edite a variável SIMILARITY_THRESHOLD no código)")
    print("=" * 60)
    
    # Configura pasta output
    output_dir, output_segments_dir = setup_output_directory()
    print(f"Pasta output criada: {output_dir}")
    print("=" * 60)
    
    # Testa com uma pasta específica
    test_path = "../../downloads/playlist_PLd6DVnxUXB2yi_HtPcjZdxA0Kna6K8-Ng/VmEu1gB3lXc/segments/"
    
    if os.path.exists(test_path):
        print("Testando com pasta específica:")
        process_validation(test_path, output_segments_dir)
        print()
    else:
        print("Pasta de teste não encontrada")
    
    print("Processando todos os diretórios:")
    batch_validate_all("../../downloads/")

if __name__ == "__main__":
    main()