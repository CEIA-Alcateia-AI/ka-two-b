#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mapeador da estrutura atual do projeto Katube
Mostra disposição de arquivos e identifica problemas de caminho
"""

import os
from pathlib import Path

def map_project_structure():
    """
    Mapeia estrutura completa do projeto partindo do diretório atual
    """
    print("MAPEAMENTO DA ESTRUTURA DO PROJETO KATUBE")
    print("=" * 60)
    
    current_dir = Path.cwd()
    print(f"Diretório atual: {current_dir}")
    
    # Busca pela raiz do projeto (onde está o .git ou README)
    project_root = find_project_root(current_dir)
    print(f"Raiz do projeto: {project_root}")
    
    print("\nESTRUTURA COMPLETA:")
    print_directory_tree(project_root, max_depth=3)
    
    # Analisa problema específico dos downloads
    analyze_download_paths(project_root)

def find_project_root(start_path):
    """
    Encontra raiz do projeto subindo na hierarquia
    """
    current = start_path
    
    for _ in range(10):  # Máximo 10 níveis para cima
        # Indica raiz do projeto
        if any((current / indicator).exists() for indicator in ['.git', 'README.md', 'src']):
            return current
        
        parent = current.parent
        if parent == current:  # Chegou na raiz do sistema
            break
        current = parent
    
    return start_path  # Fallback para diretório atual

def print_directory_tree(path, prefix="", max_depth=3, current_depth=0):
    """
    Imprime árvore de diretórios com formatação
    """
    if current_depth > max_depth:
        return
    
    path = Path(path)
    if not path.exists():
        return
    
    items = sorted(path.iterdir(), key=lambda x: (x.is_file(), x.name.lower()))
    
    for i, item in enumerate(items):
        is_last = i == len(items) - 1
        current_prefix = "└── " if is_last else "├── "
        print(f"{prefix}{current_prefix}{item.name}")
        
        if item.is_dir() and not item.name.startswith('.'):
            extension = "    " if is_last else "│   "
            print_directory_tree(
                item, 
                prefix + extension, 
                max_depth, 
                current_depth + 1
            )

def analyze_download_paths(project_root):
    """
    Analisa especificamente os caminhos de download
    """
    print("\n" + "=" * 60)
    print("ANÁLISE DOS CAMINHOS DE DOWNLOAD")
    print("=" * 60)
    
    # Verifica onde existe pasta downloads
    possible_download_locations = [
        project_root / "downloads",           # Correto - na raiz
        project_root / "src" / "downloads",   # Incorreto - dentro do src
        Path.cwd() / "downloads",            # Onde pode estar criando
    ]
    
    print("Localizações possíveis da pasta downloads:")
    for i, location in enumerate(possible_download_locations, 1):
        exists = "✅ EXISTE" if location.exists() else "❌ NÃO EXISTE"
        print(f"  {i}. {location} - {exists}")
        
        if location.exists():
            # Lista conteúdo se existir
            try:
                contents = list(location.iterdir())
                print(f"     Conteúdo: {len(contents)} itens")
                for item in contents[:3]:  # Primeiros 3 itens
                    print(f"       - {item.name}")
                if len(contents) > 3:
                    print(f"       ... e mais {len(contents) - 3} itens")
            except:
                print("     Erro ao listar conteúdo")
    
    # Analisa configurações atuais
    print("\nANÁLISE DAS CONFIGURAÇÕES ATUAIS:")
    analyze_config_paths(project_root)

def analyze_config_paths(project_root):
    """
    Analisa caminhos configurados nos arquivos de config
    """
    config_files = [
        project_root / "src" / "config.py",
        project_root / "src" / "download" / "download_config.py"
    ]
    
    for config_file in config_files:
        if config_file.exists():
            print(f"\n📁 {config_file.relative_to(project_root)}:")
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Procura por definições de path
                lines = content.split('\n')
                for i, line in enumerate(lines, 1):
                    if any(keyword in line.lower() for keyword in ['downloads', 'output', 'path']):
                        if '=' in line and not line.strip().startswith('#'):
                            print(f"  Linha {i}: {line.strip()}")
            except:
                print("  Erro ao ler arquivo")

def check_current_execution_context():
    """
    Verifica contexto de execução atual
    """
    print("\n" + "=" * 60)
    print("CONTEXTO DE EXECUÇÃO ATUAL")
    print("=" * 60)
    
    print(f"Diretório atual: {Path.cwd()}")
    print(f"Script localizado em: {Path(__file__).parent if '__file__' in globals() else 'N/A'}")
    print(f"Python sys.path[0]: {__import__('sys').path[0]}")
    
    # Testa importações
    print("\nTESTE DE IMPORTAÇÕES:")
    try:
        import sys
        src_path = Path.cwd() / "src"
        if src_path not in [Path(p) for p in sys.path]:
            sys.path.insert(0, str(src_path))
        
        # Tenta importar config
        try:
            from config import default_config
            print("✅ config.py importado com sucesso")
            print(f"   downloads base: {getattr(default_config.DOWNLOAD, 'target_url', 'N/A')}")
        except ImportError as e:
            print(f"❌ Erro ao importar config.py: {e}")
        
        # Tenta importar download_config
        try:
            from download.download_config import DownloadConfig
            test_config = DownloadConfig()
            print("✅ download_config.py importado com sucesso")
            print(f"   BASE_OUTPUT_DIR: {test_config.BASE_OUTPUT_DIR}")
            print(f"   output_dir: {test_config.output_dir}")
        except ImportError as e:
            print(f"❌ Erro ao importar download_config.py: {e}")
            
    except Exception as e:
        print(f"❌ Erro geral nos testes: {e}")

def main():
    """
    Função principal do mapeamento
    """
    map_project_structure()
    check_current_execution_context()
    
    print("\n" + "=" * 60)
    print("RESUMO DO DIAGNÓSTICO")
    print("=" * 60)
    print("1. Execute este script para identificar a estrutura atual")
    print("2. Verifique onde está criando a pasta downloads")
    print("3. Identifique se o problema está no download_config.py")
    print("4. Ajustaremos os caminhos para usar sempre a raiz do projeto")

if __name__ == "__main__":
    main()