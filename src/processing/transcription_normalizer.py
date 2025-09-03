#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Normalizador de transcrições para validação cruzada
# Versão KISS - simples e funcional
# Baseado no projeto Katube original
#

import json
import re
import unicodedata
import os
from datetime import datetime

def remove_html_tags(text):
    """
    Remove html tags from a string using regular expressions.
    Baseado em text_normalization.txt
    """
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def text_cleaning(text):
    """
    Performs a series of operations to clear the text in order to normalize it.
    Baseado em text_normalization.txt com remoção completa de acentos
    """
    # Removendo line break.
    text = text.replace('\n', ' ')

    # Removendo html tags.
    text = remove_html_tags(text)

    # Remove TODOS os acentos (á→a, ç→c, ã→a, etc)
    text = unicodedata.normalize('NFD', text)
    text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')

    # Converting to lower case
    text = text.lower()

    # Replacing ... for .
    text = re.sub("[...]+", ".", text)

    # Remove (, [
    text = re.sub("[(\[\])]+", "", text)

    # Remove space before punctuation
    text = re.sub(r'\s([.,;:?!"](?:\s|$))', r'\1', text)

    # Removendo double blank spaces
    text = re.sub("[  ]+", " ", text)

    return text.strip()

def normalize_text(text):
    """
    Normaliza texto para comparação
    """
    if not text or text.strip() == "":
        return None
    
    # Aplica limpeza baseada no projeto original
    normalized = text_cleaning(text)
    
    # Remove pontuação adicional
    punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    for char in punctuations:
        normalized = normalized.replace(char, ' ')
    
    # Remove espaços múltiplos
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized if normalized else None

def load_json_file(file_path):
    """
    Carrega arquivo JSON
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Erro ao carregar {file_path}: {e}")
        return None

def process_segments_folder(segments_path):
    """
    Processa uma pasta de segments
    """
    print(f"Processando: {segments_path}")
    
    # Caminhos dos arquivos
    lgris_file = os.path.join(segments_path, "transcricoes_lgris.json")
    freds0_file = os.path.join(segments_path, "transcricoes_freds0.json")
    output_file = os.path.join(segments_path, "normalized_transcriptions.json")
    
    # Verifica se arquivos existem
    if not os.path.exists(lgris_file):
        print(f"Arquivo não encontrado: {lgris_file}")
        return False
    
    if not os.path.exists(freds0_file):
        print(f"Arquivo não encontrado: {freds0_file}")
        return False
    
    # Carrega os JSONs
    lgris_data = load_json_file(lgris_file)
    freds0_data = load_json_file(freds0_file)
    
    if not lgris_data or not freds0_data:
        print("Erro ao carregar arquivos JSON")
        return False
    
    # Extrai transcrições
    lgris_transcriptions = lgris_data.get('transcriptions', {})
    freds0_transcriptions = freds0_data.get('transcriptions', {})
    
    print(f"Lgris: {len(lgris_transcriptions)} transcrições")
    print(f"Freds0: {len(freds0_transcriptions)} transcrições")
    
    # Processa pares
    normalized_pairs = {}
    valid_pairs = 0
    
    # Encontra todos os IDs únicos
    all_ids = set(lgris_transcriptions.keys()) | set(freds0_transcriptions.keys())
    
    for segment_id in sorted(all_ids):
        # Pega textos originais
        lgris_entry = lgris_transcriptions.get(segment_id, {})
        freds0_entry = freds0_transcriptions.get(segment_id, {})
        
        lgris_original = lgris_entry.get('text')
        freds0_original = freds0_entry.get('text')
        
        # Normaliza textos
        lgris_normalized = normalize_text(lgris_original) if lgris_original else None
        freds0_normalized = normalize_text(freds0_original) if freds0_original else None
        
        # Adiciona ao resultado
        normalized_pairs[segment_id] = {
            'lgris_original': lgris_original,
            'lgris_normalized': lgris_normalized,
            'freds0_original': freds0_original,
            'freds0_normalized': freds0_normalized,
            'segment_filename': f"{segment_id}.wav"
        }
        
        # Conta pares válidos
        if lgris_normalized and freds0_normalized:
            valid_pairs += 1
    
    # Cria estrutura final
    result = {
        "metadata": {
            "processing_date": datetime.now().isoformat(),
            "lgris_source": "transcricoes_lgris.json",
            "freds0_source": "transcricoes_freds0.json",
            "total_pairs": len(normalized_pairs),
            "valid_pairs": valid_pairs
        },
        "normalized_pairs": normalized_pairs
    }
    
    # Salva resultado
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"Arquivo salvo: {output_file}")
        print(f"Total de pares: {len(normalized_pairs)}")
        print(f"Pares válidos: {valid_pairs}")
        return True
        
    except Exception as e:
        print(f"Erro ao salvar: {e}")
        return False

def batch_process_all(base_dir):
    """
    Processa todos os diretórios de segments
    """
    print("Procurando diretórios de segments...")
    
    processed = 0
    for root, dirs, files in os.walk(base_dir):
        if 'transcricoes_lgris.json' in files and 'transcricoes_freds0.json' in files:
            if process_segments_folder(root):
                processed += 1
            print("-" * 50)
    
    print(f"Total processado: {processed} diretórios")

def main():
    """
    Função principal
    """
    print("KATUBE TRANSCRIPTION NORMALIZER - Versão KISS")
    print("=" * 50)
    
    # Testa com uma pasta específica
    test_path = "../../downloads/playlist_PLd6DVnxUXB2yi_HtPcjZdxA0Kna6K8-Ng/VmEu1gB3lXc/segments/"
    
    if os.path.exists(test_path):
        print("Testando com pasta específica:")
        process_segments_folder(test_path)
    else:
        print("Pasta de teste não encontrada")
        
    print("\nProcessando todos os diretórios:")
    batch_process_all("../../downloads/")

if __name__ == "__main__":
    main()