#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Teste para verificar se o modelo freds0 suporta timestamps
Script SEPARADO que não afeta o código funcionando
"""

import sys
import os
from pathlib import Path

# Adiciona caminho para importar módulo de transcrição
current_dir = Path(__file__).parent
transcription_path = current_dir.parent / "transcription"
sys.path.insert(0, str(transcription_path))

try:
    from freds0_transcriber import Freds0Transcriber
except ImportError as e:
    print(f"Erro ao importar transcriber: {e}")
    print("Verifique se o arquivo freds0_transcriber.py existe em src/transcription/")
    sys.exit(1)


def test_timestamps_support():
    """
    Testa se o modelo freds0 suporta timestamps detalhados
    Não modifica nada do código existente
    """
    print("=" * 60)
    print("TESTE DE SUPORTE A TIMESTAMPS - MODELO FREDS0")
    print("=" * 60)
    
    # Procura um arquivo de áudio para teste
    test_audio = find_test_audio()
    
    if not test_audio:
        print("Nenhum arquivo de áudio encontrado para teste")
        print("Coloque um arquivo .wav ou .mp3 na pasta downloads/ para testar")
        return False
    
    print(f"Arquivo de teste: {test_audio}")
    print("\n1. Testando transcrição NORMAL (sem timestamps)...")
    
    try:
        # Cria instância do transcriber (mesmo que código funcionando)
        transcriber = Freds0Transcriber()
        
        if not transcriber.load_model():
            print("Falha ao carregar modelo")
            return False
        
        # Teste 1: Transcrição normal (para garantir que não quebramos nada)
        print("Executando transcrição normal...")
        result_normal = transcriber.transcribe(test_audio)
        
        if "error" in result_normal:
            print(f"Erro na transcrição normal: {result_normal['error']}")
            return False
        
        print("✅ Transcrição normal funcionando:")
        print(f"Texto: {result_normal['text'][:100]}...")
        
        # Teste 2: Verificar se pipeline suporta timestamps
        print("\n2. Testando suporte a TIMESTAMPS...")
        
        # Tenta usar return_timestamps diretamente no pipeline
        try:
            result_with_timestamps = transcriber.pipe(
                test_audio,
                return_timestamps=True
            )
            
            print("✅ Pipeline aceita return_timestamps=True")
            
            # Analisa estrutura do resultado
            if isinstance(result_with_timestamps, dict):
                print(f"Resultado é dict com chaves: {result_with_timestamps.keys()}")
                
                if "chunks" in result_with_timestamps:
                    chunks = result_with_timestamps["chunks"]
                    print(f"Encontrados {len(chunks)} chunks com timestamps")
                    
                    # Mostra exemplo de chunk
                    if chunks and len(chunks) > 0:
                        example_chunk = chunks[0]
                        print(f"Exemplo de chunk: {example_chunk}")
                        
                        # Verifica se tem timestamps detalhados
                        if "timestamp" in example_chunk:
                            print("✅ SUPORTE A TIMESTAMPS CONFIRMADO!")
                            return True
                        else:
                            print("⚠️ Chunks sem timestamps detalhados")
                            return False
                    else:
                        print("⚠️ Lista de chunks vazia")
                        return False
                else:
                    print("⚠️ Resultado não contém 'chunks'")
                    print(f"Estrutura: {result_with_timestamps}")
                    return False
            else:
                print(f"⚠️ Resultado não é dict: {type(result_with_timestamps)}")
                return False
                
        except Exception as e:
            print(f"❌ Pipeline não suporta timestamps: {e}")
            return False
            
    except Exception as e:
        print(f"Erro no teste: {e}")
        return False


def find_test_audio():
    """
    Procura arquivo de áudio para teste na estrutura do projeto
    """
    # Procura na pasta downloads
    downloads_dir = Path("../../downloads")
    
    if downloads_dir.exists():
        # Busca qualquer .mp3 ou .wav
        for audio_file in downloads_dir.rglob("*.mp3"):
            return str(audio_file)
        for audio_file in downloads_dir.rglob("*.wav"):
            return str(audio_file)
    
    # Fallback: procura no diretório atual
    current = Path(".")
    for audio_file in current.rglob("*.mp3"):
        return str(audio_file)
    for audio_file in current.rglob("*.wav"):
        return str(audio_file)
    
    return None


def main():
    """
    Função principal - executa todos os testes
    """
    print("Iniciando teste de timestamps...")
    
    # Teste principal
    timestamps_supported = test_timestamps_support()
    
    print("\n" + "=" * 60)
    print("RESULTADO FINAL DO TESTE")
    print("=" * 60)
    
    if timestamps_supported:
        print("✅ MODELO FREDS0 SUPORTA TIMESTAMPS!")
        print("👉 Podemos usar Opção C (Whisper com Timestamps)")
        print("👉 Alinhamento será preciso e eficiente")
    else:
        print("❌ Modelo freds0 NÃO suporta timestamps adequados")
        print("👉 Vamos usar Opção B (Alinhamento temporal simples)")
        print("👉 Alinhamento baseado em matemática de timestamps")
    
    print("\n💡 Código de transcrição existente não foi modificado")
    print("💡 Este teste não afeta nada do funcionamento atual")


if __name__ == "__main__":
    main()