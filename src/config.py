#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuração Centralizada do Pipeline Katube
Sistema modular para geração de datasets TTS/STT a partir do YouTube
Versão: 1.0 - Implementação gradual com compatibilidade total
"""

from dataclasses import dataclass
from typing import List, Dict, Optional


class KatubeConfig:
    """
    Configuração centralizada para todo o pipeline Katube
    Permite controle granular de cada etapa do processamento
    """
    
    # ========================================================================
    # CONTROLE DO PIPELINE - Quais etapas executar
    # ========================================================================
    
    PIPELINE_STEPS = {
        'download': True,        # Baixar áudios e legendas do YouTube
        'segment': True,         # Segmentar áudio em chunks usando VAD
        'transcribe': True,      # Transcrever segmentos com 2 modelos
        'normalize': True,       # Normalizar transcrições para comparação
        'validate': True,        # Validar usando similaridade cruzada
        'cleanup': False         # Limpar arquivos intermediários
    }
    
    # ========================================================================
    # CONFIGURAÇÕES DE DOWNLOAD - Parâmetros essenciais
    # ========================================================================
    
    DOWNLOAD = {
        # URL para processamento (vídeo, playlist ou canal)
        'target_url': "https://www.youtube.com/watch?v=rnQUKvjucxg",  # Exemplo: "https://www.youtube.com/playlist?list=PLxxx"
        
        # Controle de quantidade
        'limit': 0,  # 0 = todos os vídeos, N = máximo N vídeos
        
        # Qualidade de áudio
        'audio_format': "mp3",    # Opções: "mp3", "wav", "m4a", "flac"
        'audio_quality': 0,       # kbps: 128, 192, 256, 320 (0 = melhor disponível)
        
        # Anti-bloqueio (delays entre downloads)
        'delay_min_seconds': 15,  # Mínimo recomendado: 10s
        'delay_max_seconds': 30,  # Máximo recomendado: 60s
        
        # Legendas
        'subtitle_languages': ["pt-BR", "pt"],  # Prioridade: pt-BR > pt
        'download_auto_subs': True,             # Baixar legendas automáticas se manual não existir
        
        # Filtros de duração de vídeo
        'min_duration_seconds': 30,    # Mínimo: 30s (evita vídeos muito curtos)
        'max_duration_seconds': 3600,  # Máximo: 1 hora (evita vídeos muito longos)
        
        # Comportamento
        'overwrite_existing': False    # False = pula vídeos já baixados
    }
    
    # ========================================================================
    # CONFIGURAÇÕES DE SEGMENTAÇÃO - VAD (Voice Activity Detection)
    # ========================================================================
    
    SEGMENTATION = {
        # Duração dos segmentos
        'min_duration_sec': 4.0,   # Mínimo: 4s (segmentos curtos são descartados)
        'max_duration_sec': 15.0,  # Máximo: 15s (segmentos longos são subdivididos)
        
        # Parâmetros técnicos do VAD
        'sampling_rate': 16000,          # Taxa para análise VAD (Silero padrão)
        'target_sampling_rate': 24000,   # Taxa final dos segmentos salvos  
        'window_size_seconds': 0.15,     # Janela de análise: 150ms
        
        # Controle de qualidade
        'min_silence_for_split': 0.3,    # 300ms de silêncio para divisão
        'voice_threshold': 0.5           # Limiar de confiança VAD (0.0-1.0)
    }
    
    # ========================================================================
    # CONFIGURAÇÕES DE TRANSCRIÇÃO - Modelos freds0 (Whisper)
    # ========================================================================
    
    TRANSCRIPTION_FREDS0 = {
        # Modelo Whisper português
        'model_name': "freds0/distil-whisper-large-v3-ptbr",
        
        # Processamento
        'chunk_length_s': 30,            # Segundos por chunk (ótimo para Whisper)
        'stride_length_s': 5,            # Sobreposição entre chunks
        'return_timestamps': True,       # Timestamps internos do modelo
        
        # Qualidade
        'min_audio_duration': 1.0,       # Mínimo em segundos
        'max_audio_duration': 60.0,      # Máximo em segundos
        
        # Output
        'output_filename': "transcricoes_freds0.json",
        'overwrite_existing': False      # Não sobrescreve por padrão
    }
    
    # ========================================================================
    # CONFIGURAÇÕES DE TRANSCRIÇÃO - Modelos lgris (Wav2Vec2)
    # ========================================================================
    
    TRANSCRIPTION_LGRIS = {
        # Modelo Wav2Vec2 português
        'model_name': "lgris/wav2vec2-large-xlsr-open-brazilian-portuguese",
        'target_sample_rate': 16000,     # Taxa requerida pelo Wav2Vec2
        
        # Processamento
        'batch_size': 1,                 # Individual para estabilidade
        'chunk_length': 30,              # Segundos por chunk
        
        # Qualidade
        'min_audio_duration': 1.0,       # Mínimo em segundos
        'max_audio_duration': 30.0,      # Máximo em segundos
        
        # Output
        'output_filename': "transcricoes_lgris.json",
        'overwrite_existing': False      # Não sobrescreve por padrão
    }
    
    # ========================================================================
    # CONFIGURAÇÕES DE NORMALIZAÇÃO - Preparação para validação cruzada
    # ========================================================================
    
    NORMALIZATION = {
        # Controle principal (único parâmetro editável)
        'enabled': True,                 # True = normaliza texto para comparação
                                        # False = pula normalização (não recomendado)
        
        # NOTA: Parâmetros técnicos da normalização são fixos e otimizados
        # para máxima compatibilidade entre modelos de transcrição
    }
    
    # ========================================================================
    # CONFIGURAÇÕES DE VALIDAÇÃO - Similaridade cruzada
    # ========================================================================
    
    VALIDATION = {
        # Threshold de aprovação (parâmetro crítico)
        'similarity_threshold': 0.9,     # Valores sugeridos:
                                         # 0.6 = 60% (mais permissivo, mais dados)
                                         # 0.7 = 70% (balanceado) - RECOMENDADO
                                         # 0.8 = 80% (mais rigoroso, menos dados)
                                         # 0.9 = 90% (muito rigoroso)
    }
    
    # ========================================================================
    # CONFIGURAÇÕES DE LIMPEZA - Cleanup de arquivos intermediários
    # ========================================================================
    
    CLEANUP = {
        # Controle principal
        'enabled': True,                # False = segurança máxima (nada é removido)
                                        # True = remove arquivos intermediários
                                        # CUIDADO: True remove permanentemente!
        
        # Preservação de dados
        'keep_execution_reports': True   # True = mantém execution_report.json
                                        # False = remove tudo exceto output/
        
        # NOTA: Se enabled=True, cleanup executa automaticamente após pipeline
        # Não há opção manual - se usuário habilita, aceita limpeza automática
    }
    
    # ========================================================================
    # SEPARAÇÃO: CONFIGURAÇÕES NÃO-EDITÁVEIS PELO USUÁRIO
    # ========================================================================
    
    """
    ⚠️ ATENÇÃO: CONFIGURAÇÕES ABAIXO SÃO INTERNAS DO SISTEMA
    
    As configurações abaixo são otimizadas para funcionamento estável do pipeline
    e NÃO devem ser modificadas pelo usuário comum, pois:
    
    - São baseadas em benchmarks e testes extensivos
    - Modificações podem quebrar a pipeline ou degradar qualidade
    - Representam valores técnicos específicos dos modelos de IA
    - Estão ajustadas para máxima compatibilidade entre módulos
    
    Se você tem necessidades avançadas específicas, modifique diretamente
    os arquivos de configuração individuais dos módulos correspondentes.
    """
    
    # Filtros automáticos de conteúdo (configuração interna)
    _CONTENT_FILTERS = {
        'skip_live_streams': True,       # Pula transmissões ao vivo
        'skip_premieres': False,         # Pula premieres  
        'skip_shorts': False             # Pula YouTube Shorts
    }
    
    # Configurações de retry (configuração interna)
    _RETRY_CONFIG = {
        'max_retries': 3,                # Tentativas por vídeo
        'retry_delay': 60                # Segundos entre tentativas
    }
    
    # ========================================================================
    # MÉTODOS DE VALIDAÇÃO E UTILITÁRIOS
    # ========================================================================
    
    def validate_config(self) -> Dict[str, any]:
        """
        Valida todas as configurações e retorna relatório
        Garante que parâmetros estão em ranges válidos
        
        Returns:
            Dict: Relatório de validação com erros/avisos
        """
        validation = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Valida URL (se fornecida)
        if self.DOWNLOAD['target_url']:
            if not any(domain in self.DOWNLOAD['target_url'] for domain in ['youtube.com', 'youtu.be']):
                validation['errors'].append("URL deve ser do YouTube (youtube.com ou youtu.be)")
                validation['valid'] = False
        else:
            validation['warnings'].append("URL não configurada - necessária para download")
        
        # Valida qualidade de áudio
        valid_qualities = [0, 128, 192, 256, 320]
        if self.DOWNLOAD['audio_quality'] not in valid_qualities:
            validation['errors'].append(f"Qualidade inválida: {self.DOWNLOAD['audio_quality']}. Use: {valid_qualities}")
            validation['valid'] = False
        
        # Valida formato de áudio
        valid_formats = ["mp3", "wav", "m4a", "flac"]
        if self.DOWNLOAD['audio_format'] not in valid_formats:
            validation['errors'].append(f"Formato inválido: {self.DOWNLOAD['audio_format']}. Use: {valid_formats}")
            validation['valid'] = False
        
        # Valida delays
        if self.DOWNLOAD['delay_min_seconds'] > self.DOWNLOAD['delay_max_seconds']:
            validation['errors'].append("delay_min_seconds deve ser menor que delay_max_seconds")
            validation['valid'] = False
        
        # Valida threshold de similaridade
        if not (0.0 <= self.VALIDATION['similarity_threshold'] <= 1.0):
            validation['errors'].append("similarity_threshold deve estar entre 0.0 e 1.0")
            validation['valid'] = False
        
        # Valida durações de segmentação
        if self.SEGMENTATION['min_duration_sec'] > self.SEGMENTATION['max_duration_sec']:
            validation['errors'].append("min_duration_sec deve ser menor que max_duration_sec")
            validation['valid'] = False
        
        # Avisos úteis
        if self.DOWNLOAD['limit'] == 0:
            validation['warnings'].append("limit = 0: Baixará TODOS os vídeos da playlist/canal")
        
        if self.CLEANUP['enabled']:
            validation['warnings'].append("CLEANUP habilitado: Arquivos intermediários serão removidos!")
        
        return validation
    
    def get_pipeline_summary(self) -> Dict[str, any]:
        """
        Retorna resumo das configurações do pipeline
        Útil para exibição e confirmação antes da execução
        
        Returns:
            Dict: Resumo formatado das configurações
        """
        active_steps = [step for step, enabled in self.PIPELINE_STEPS.items() if enabled]
        
        return {
            'pipeline_steps_active': active_steps,
            'total_steps': len(active_steps),
            'download_config': {
                'url': self.DOWNLOAD['target_url'] or "NÃO CONFIGURADA",
                'limit': self.DOWNLOAD['limit'] if self.DOWNLOAD['limit'] > 0 else "Todos",
                'format': f"{self.DOWNLOAD['audio_format'].upper()} @ {self.DOWNLOAD['audio_quality']} kbps" if self.DOWNLOAD['audio_quality'] > 0 else f"{self.DOWNLOAD['audio_format'].upper()} @ Melhor disponível",
                'delays': f"{self.DOWNLOAD['delay_min_seconds']}-{self.DOWNLOAD['delay_max_seconds']}s"
            },
            'validation_threshold': f"{self.VALIDATION['similarity_threshold']:.1%}",
            'cleanup_enabled': self.CLEANUP['enabled'],
            'estimated_safety_level': self._calculate_safety_level()
        }
    
    def _calculate_safety_level(self) -> str:
        """Calcula nível de segurança baseado nas configurações"""
        safety_score = 0
        
        # Delays adequados
        if self.DOWNLOAD['delay_min_seconds'] >= 10:
            safety_score += 1
        
        # Não sobrescreve existentes
        if not self.DOWNLOAD['overwrite_existing']:
            safety_score += 1
        
        # Cleanup desabilitado
        if not self.CLEANUP['enabled']:
            safety_score += 1
        
        # Threshold conservador
        if self.VALIDATION['similarity_threshold'] >= 0.7:
            safety_score += 1
        
        if safety_score >= 4:
            return "ALTO (Configuração conservadora)"
        elif safety_score >= 2:
            return "MÉDIO (Configuração balanceada)" 
        else:
            return "BAIXO (Configuração agressiva)"


# ========================================================================
# INSTÂNCIA GLOBAL PADRÃO - Para importação simples nos módulos
# ========================================================================

# Instância padrão para uso nos módulos existentes
default_config = KatubeConfig()


# ========================================================================
# FUNÇÕES DE CONVENIÊNCIA - Para uso em scripts externos
# ========================================================================

def get_config() -> KatubeConfig:
    """
    Retorna instância da configuração padrão
    Função de conveniência para importação simples
    
    Returns:
        KatubeConfig: Instância configurada
    """
    return default_config


def create_custom_config(**kwargs) -> KatubeConfig:
    """
    Cria configuração personalizada com overrides
    
    Args:
        **kwargs: Parâmetros para sobrescrever
        
    Returns:
        KatubeConfig: Nova instância com configurações customizadas
        
    Example:
        config = create_custom_config(
            target_url="https://youtube.com/playlist?list=xyz",
            similarity_threshold=0.8,
            cleanup_enabled=True
        )
    """
    config = KatubeConfig()
    
    # Aplica overrides nos grupos de configuração
    for key, value in kwargs.items():
        if key == 'target_url':
            config.DOWNLOAD['target_url'] = value
        elif key == 'similarity_threshold':
            config.VALIDATION['similarity_threshold'] = value
        elif key == 'cleanup_enabled':
            config.CLEANUP['enabled'] = value
        # Adicionar mais overrides conforme necessário
    
    return config


def validate_and_show_config(config: KatubeConfig = None) -> bool:
    """
    Valida configuração e exibe relatório
    
    Args:
        config: Configuração a validar (usa padrão se None)
        
    Returns:
        bool: True se configuração válida
    """
    if config is None:
        config = default_config
    
    validation = config.validate_config()
    summary = config.get_pipeline_summary()
    
    print("=" * 60)
    print("KATUBE CONFIG - RELATÓRIO DE CONFIGURAÇÃO")
    print("=" * 60)
    
    print(f"Etapas ativas: {', '.join(summary['pipeline_steps_active'])}")
    print(f"URL: {summary['download_config']['url']}")
    print(f"Limite: {summary['download_config']['limit']}")
    print(f"Qualidade: {summary['download_config']['format']}")
    print(f"Delays: {summary['download_config']['delays']}")
    print(f"Threshold validação: {summary['validation_threshold']}")
    print(f"Cleanup: {'Habilitado' if summary['cleanup_enabled'] else 'Desabilitado'}")
    print(f"Nível de segurança: {summary['estimated_safety_level']}")
    
    if validation['valid']:
        print("\nCONFIGURAÇÃO VÁLIDA!")
    else:
        print("\nERROS ENCONTRADOS:")
        for error in validation['errors']:
            print(f"  - {error}")
    
    if validation['warnings']:
        print("\nAVISOS:")
        for warning in validation['warnings']:
            print(f"  - {warning}")
    
    print("=" * 60)
    
    return validation['valid']


# ========================================================================
# EXECUÇÃO STANDALONE - Para testes
# ========================================================================

def main():
    """
    Função principal para execução standalone
    Mostra configuração padrão e validação
    """
    print("KATUBE CONFIG - Sistema de Configuração Centralizada")
    print("Versão: 1.0 - Implementação Gradual")
    print()
    
    # Testa configuração padrão
    is_valid = validate_and_show_config()
    
    if is_valid:
        print("Sistema pronto para uso!")
    else:
        print("Corrija os erros antes de executar o pipeline.")


if __name__ == "__main__":
    main()