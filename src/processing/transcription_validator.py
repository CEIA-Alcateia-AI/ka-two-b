#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Validador de transcriÃ§Ãµes usando distÃ¢ncia Levenshtein
# VersÃ£o KISS - simples e funcional
# Baseado no projeto Katube original (validation.txt)
#

import json
import os
import csv
import shutil
from datetime import datetime
from textdistance import levenshtein

# =============================================================================
# CONFIGURAÃ‡ÃƒO VIA CONFIG.PY - ConfiguraÃ§Ã£o centralizada
# =============================================================================

# Importa configuraÃ§Ãµes do sistema centralizado
try:
    import sys
    import os
    # Adiciona o diretÃ³rio src ao path de forma robusta
    src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)))
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    from config import default_config
    
    # Threshold de similaridade vindo do config.py centralizado
    SIMILARITY_THRESHOLD = default_config.VALIDATION['similarity_threshold']
    
    print(f"Threshold de validaÃ§Ã£o carregado do config.py: {SIMILARITY_THRESHOLD:.1%}")
    
except ImportError as e:
    print(f"Aviso: NÃ£o foi possÃ­vel importar config.py, usando threshold padrÃ£o: {e}")
    # Fallback para valor padrÃ£o se config.py nÃ£o estiver disponÃ­vel
    SIMILARITY_THRESHOLD = 0.7  # Valor conservador por seguranÃ§a

# =============================================================================
def find_project_root():
    """
    Encontra a pasta raiz do projeto procurando pela pasta downloads
    """
    current_dir = os.path.abspath(os.getcwd())
    
    # Sobe atÃ© encontrar a pasta downloads ou chegar na raiz
    for _ in range(5):  # MÃ¡ximo 5 nÃ­veis
        if os.path.exists(os.path.join(current_dir, 'downloads')):
            return current_dir
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:  # Chegou na raiz do sistema
            break
        current_dir = parent_dir
    
    # Se nÃ£o encontrar, usa diretÃ³rio atual
    return os.path.abspath('.')

def setup_output_directory():
    """
    Cria estrutura da pasta output
    """
    project_root = find_project_root()
    output_dir = os.path.join(project_root, 'output')
    segments_dir = os.path.join(output_dir, 'segments')
    
    # Cria diretÃ³rios se nÃ£o existirem
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
    
    # Remove espaÃ§os extras para comparaÃ§Ã£o mais justa
    clean_text1 = text1.strip()
    clean_text2 = text2.strip()
    
    if not clean_text1 or not clean_text2:
        return 0.0
    
    # Calcula similaridade normalizada (0.0 a 1.0)
    similarity = levenshtein.normalized_similarity(clean_text1, clean_text2)
    return similarity

def load_existing_dataset(final_csv_path):
    """
    Carrega dataset existente para append inteligente
    Retorna dicionario com dados existentes indexados por filename
    """
    existing_data = {}
    
    if not os.path.exists(final_csv_path):
        print("Dataset final nao existe ainda, criando novo...")
        return existing_data
    
    try:
        with open(final_csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                filename = row.get('filename', '')
                if filename:
                    existing_data[filename] = row
        
        print(f"Carregados {len(existing_data)} registros existentes do dataset")
        return existing_data
        
    except Exception as e:
        print(f"Erro ao carregar dataset existente: {e}")
        return {}

def process_validation(segments_path, output_segments_dir):
    """
    Processa validaÃ§Ã£o de uma pasta de segments
    Retorna lista de arquivos aprovados para consolidaÃ§Ã£o
    """
    print(f"Validando: {segments_path}")
    
    # Arquivos de entrada e saÃ­da
    input_file = os.path.join(segments_path, "normalized_transcriptions.json")
    results_file = os.path.join(segments_path, "validation_results.json")
    csv_file = os.path.join(segments_path, "approved_dataset.csv")
    
    # Verifica se arquivo de entrada existe
    if not os.path.exists(input_file):
        print(f"Arquivo nÃ£o encontrado: {input_file}")
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
    
    # EstatÃ­sticas
    total_pairs = 0
    approved_pairs = 0
    rejected_pairs = 0
    invalid_pairs = 0
    
    # Lista para CSV final
    approved_data = []
    
    # Processa cada par
    for segment_id, pair_data in normalized_pairs.items():
        total_pairs += 1
        
        # Pega textos normalizados para comparaÃ§Ã£o
        lgris_normalized = pair_data.get('lgris_normalized')
        freds0_normalized = pair_data.get('freds0_normalized')
        
        # Pega textos originais para salvar no dataset
        lgris_original = pair_data.get('lgris_original')
        freds0_original = pair_data.get('freds0_original')
        
        # Verifica se textos sÃ£o vÃ¡lidos
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
            
            # Copia arquivo de Ã¡udio aprovado para pasta output
            source_audio = os.path.join(segments_path, f"{segment_id}.wav")
            if os.path.exists(source_audio):
                try:
                    dest_audio = os.path.join(output_segments_dir, f"{segment_id}.wav")
                    shutil.copy2(source_audio, dest_audio)
                    print(f"Audio copiado: {segment_id}.wav -> output/segments/")
                except Exception as e:
                    print(f"Erro ao copiar {source_audio}: {e}")
            else:
                print(f"Aviso: Audio nao encontrado: {source_audio}")
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
    
    # Cria relatÃ³rio de validaÃ§Ã£o
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
    
    # Salva relatÃ³rio JSON
    try:
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(validation_results, f, indent=2, ensure_ascii=False)
        
        print(f"RelatÃ³rio salvo: {results_file}")
        
    except Exception as e:
        print(f"Erro ao salvar relatÃ³rio: {e}")
        return []
    
    # Exibe estatÃ­sticas
    print("=" * 40)
    print(f"RESULTADOS DA VALIDAÃ‡ÃƒO:")
    print(f"  Total de pares: {total_pairs}")
    print(f"  Aprovados: {approved_pairs}")
    print(f"  Rejeitados: {rejected_pairs}")
    print(f"  InvÃ¡lidos: {invalid_pairs}")
    print(f"  Taxa de aprovaÃ§Ã£o: {validation_results['metadata']['approval_rate']}%")
    print(f"  Arquivos de Ã¡udio copiados: {approved_pairs}")
    
    return approved_data

def consolidate_datasets(all_approved_data, output_dir):
    """
    Consolida todos os dados aprovados em um CSV Ãºnico
    VERSAO CORRIGIDA: Implementa append inteligente preservando dados anteriores
    """
    if not all_approved_data:
        print("Nenhum dado aprovado desta execucao para consolidar")
        # IMPORTANTE: Nao retorna aqui - ainda precisa verificar dados existentes
    
    final_csv = os.path.join(output_dir, 'final_dataset.csv')
    
    # Carrega dados existentes
    existing_data = load_existing_dataset(final_csv)
    
    # Combina dados existentes + novos (sem duplicatas)
    combined_data = existing_data.copy()  # Inicia com dados existentes
    
    duplicates_found = 0
    new_entries = 0
    
    # Adiciona novos dados, sobrescrevendo duplicatas se houver
    for new_entry in all_approved_data:
        filename = new_entry['filename']
        
        if filename in combined_data:
            duplicates_found += 1
            print(f"Atualizando registro existente: {filename}")
        else:
            new_entries += 1
            print(f"Adicionando novo registro: {filename}")
        
        combined_data[filename] = new_entry
    
    # Converte de volta para lista
    final_data = list(combined_data.values())
    
    # SEMPRE salva o arquivo final, mesmo se nao houver dados novos
    try:
        with open(final_csv, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['filename', 'lgris_text', 'freds0_text', 'similarity']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for row in final_data:
                writer.writerow(row)
        
        print(f"Dataset final consolidado salvo: {final_csv}")
        print(f"Total de registros no dataset: {len(final_data)}")
        
        if len(existing_data) > 0:
            print(f"Registros existentes preservados: {len(existing_data)}")
        if new_entries > 0:
            print(f"Registros novos adicionados: {new_entries}")
        if duplicates_found > 0:
            print(f"Registros atualizados: {duplicates_found}")
        
    except Exception as e:
        print(f"Erro ao salvar dataset final: {e}")

def batch_validate_all(base_dir):
    """
    Valida todos os diretÃ³rios de segments e consolida resultados
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
    all_approved_data = []  # Lista consolidada de todos os dados aprovados NOVOS
    
    for root, dirs, files in os.walk(base_dir):
        if 'normalized_transcriptions.json' in files:
            approved_data = process_validation(root, output_segments_dir)
            if approved_data:
                processed += 1
                all_approved_data.extend(approved_data)  # Adiciona Ã  lista consolidada
            
            print("-" * 60)
    
    # Consolida todos os dados em um CSV Ãºnico
    consolidate_datasets(all_approved_data, output_dir)
    
    print(f"RESUMO GERAL:")
    print(f"  DiretÃ³rios processados nesta execucao: {processed}")
    print(f"  Total de pares aprovados (novos): {len(all_approved_data)}")
    print(f"  Threshold usado: {SIMILARITY_THRESHOLD}")
    print(f"  Arquivos de Ã¡udio copiados (novos): {len(all_approved_data)}")
    print(f"  Dataset final: {os.path.join(output_dir, 'final_dataset.csv')}")
    print(f"  Pasta de Ã¡udios: {output_segments_dir}")

def main():
    """
    FunÃ§Ã£o principal
    """
    print("KATUBE TRANSCRIPTION VALIDATOR - VersÃ£o KISS")
    print("ValidaÃ§Ã£o cruzada usando distÃ¢ncia Levenshtein")
    print("=" * 60)
    print(f"Threshold configurado: {SIMILARITY_THRESHOLD}")
    print("(Para alterar, edite a variÃ¡vel SIMILARITY_THRESHOLD no cÃ³digo)")
    print("=" * 60)
    
    # Configura pasta output
    output_dir, output_segments_dir = setup_output_directory()
    print(f"Pasta output criada: {output_dir}")
    print("=" * 60)
    
    # Testa com uma pasta especÃ­fica
    test_path = "../../downloads/playlist_PLd6DVnxUXB2yi_HtPcjZdxA0Kna6K8-Ng/VmEu1gB3lXc/segments/"
    
    if os.path.exists(test_path):
        print("Testando com pasta especÃ­fica:")
        process_validation(test_path, output_segments_dir)
        print()
    else:
        print("Pasta de teste nÃ£o encontrada")
    
    print("Processando todos os diretÃ³rios:")
    batch_validate_all("../../downloads/")

if __name__ == "__main__":
    main()