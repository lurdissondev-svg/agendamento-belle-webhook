"""
Webhook Server para processar agendamentos do Bitrix.

Recebe dados do workflow do Bitrix, envia para Belle Software,
atualiza o lead e move para etapa "Agendados".

Uso:
    uvicorn agendamento_webhook:app --host 0.0.0.0 --port 8000 --reload
"""

import re
from datetime import datetime
from typing import Any

import httpx
import structlog
from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

logger = structlog.get_logger()

app = FastAPI(
    title="Agendamento Webhook",
    description="Processa agendamentos do Bitrix para Belle Software",
    version="1.0.0",
)

# URLs e Tokens
# A API da Belle pode usar diferentes bases dependendo do endpoint
BELLE_BASE_URL = "https://app.bellesoftware.com.br"
BELLE_TOKEN = "f236029cecd084712f7b3ce12c3e0c14"
BITRIX_WEBHOOK_URL = "https://crepaldi.bitrix24.com.br/rest/126490/7ckld4dli6jds9b2"

# ==========================================
# CAMPOS DO LEAD NO BITRIX
# ==========================================
LEAD_FIELD_DATA_AGENDAMENTO = "UF_CRM_1729176556443"      # datetime - Data do Agendamento
LEAD_FIELD_PROFISSIONAL = "UF_CRM_1729176701"            # iblock_element - Profissional! (atual)
LEAD_FIELD_ESTABELECIMENTO = "UF_CRM_6531923E155B0"      # iblock_element - Estabelecimento
LEAD_FIELD_CODIGO_AGENDAMENTO = "UF_CRM_1729176843003"   # string - Código Agendamento Belle
LEAD_FIELD_PROCEDIMENTO = "UF_CRM_1729180875"            # iblock_element - Procedimento (ativo)
LEAD_FIELD_PROCEDIMENTO_NOME = "UF_CRM_1691074328"       # string - Nome (serviço)
LEAD_FIELD_CODIGO_CLIENTE_BELLE = "UF_CRM_1730401249284" # string - Código do Cliente no Belle
LEAD_FIELD_ORIGEM = "UF_CRM_1692640693814"               # enumeration - Origem do Lead
LEAD_FIELD_CAMPANHA = "UF_CRM_1729176132205"             # enumeration - Campanha
LEAD_FIELD_TIPO_ATENDIMENTO = "UF_CRM_1695303850778"     # enumeration - Tipo de Atendimento
LEAD_FIELD_TIPO_PACIENTE = "UF_CRM_1729176219395"        # enumeration - Tipo de Paciente
LEAD_FIELD_AGENDADOR = "UF_CRM_1729176345"               # employee - Agendador
LEAD_FIELD_SEGMENTO = "UF_CRM_1729176820"                # iblock_element - Segmento (atual)
LEAD_FIELD_EQUIPAMENTO = "UF_CRM_1729176824131"          # string - Equipamento
LEAD_FIELD_TIPO_CONSULTA = "UF_CRM_1729176810161"        # string - Tipo Consulta
FIELD_CODIGO_CLIENTE_BELLE_CONTATO = "UF_CRM_1693232565211"  # Campo no Contato

# ==========================================
# CAMPOS DO DEAL (NEGÓCIO) NO BITRIX
# IMPORTANTE: Os IDs dos campos do Deal são DIFERENTES dos do Lead!
# Cada campo tem seu próprio ID no Deal.
# ==========================================
DEAL_FIELD_DATA_AGENDAMENTO = "UF_CRM_67279644B07C8"            # Data do Agendamento (datetime)
DEAL_FIELD_PROFISSIONAL = "UF_CRM_67279644D2A21"               # Profissional! (atual) (iblock_element)
DEAL_FIELD_ESTABELECIMENTO = "UF_CRM_6531923E155B0"            # Estabelecimento (iblock_element) - mesmo ID
DEAL_FIELD_CODIGO_AGENDAMENTO = "UF_CRM_6727964521E7A"         # Código do Agendamento (string)
DEAL_FIELD_PROCEDIMENTO = "UF_CRM_672796455B831"               # Procedimento (ativo) (iblock_element)
DEAL_FIELD_PROCEDIMENTO_NOME = "UF_CRM_1691074328"             # Nome (serviço) (string) - mesmo ID
DEAL_FIELD_CODIGO_CLIENTE_BELLE = "UF_CRM_672796456F150"       # Código do Cliente no Belle (string)
DEAL_FIELD_ORIGEM = "UF_CRM_64E79DD16B294"                     # Origem do lead (enumeration) - DIFERENTE!
DEAL_FIELD_CAMPANHA = "UF_CRM_1720714625351"                   # Campanha (enumeration) - DIFERENTE!
DEAL_FIELD_TIPO_ATENDIMENTO = "UF_CRM_650C5648BE36D"           # Tipo de Atendimento (enumeration) - DIFERENTE!
DEAL_FIELD_SEGMENTO = "UF_CRM_67279644E834B"                   # Segmento (atual) (iblock_element)
DEAL_FIELD_AGENDADOR = "UF_CRM_1697667153"                     # Agendador (employee) - ID DIFERENTE!
DEAL_FIELD_TIPO_PACIENTE = "UF_CRM_65118CE0CE1BD"              # Tipo de Paciente (enumeration) - DIFERENTE!
DEAL_FIELD_EQUIPAMENTO = "UF_CRM_1729176824131"                # Equipamento (string) - mesmo ID

# Mapeamento Lead -> Deal (campos personalizados)
# IMPORTANTE: Os IDs são DIFERENTES entre Lead e Deal!
# Este mapeamento converte o campo do Lead para o campo correspondente no Deal.
LEAD_TO_DEAL_FIELD_MAP = {
    LEAD_FIELD_DATA_AGENDAMENTO: DEAL_FIELD_DATA_AGENDAMENTO,       # UF_CRM_1729176556443 -> UF_CRM_67279644B07C8
    LEAD_FIELD_PROFISSIONAL: DEAL_FIELD_PROFISSIONAL,               # UF_CRM_1729176701 -> UF_CRM_67279644D2A21
    LEAD_FIELD_ESTABELECIMENTO: DEAL_FIELD_ESTABELECIMENTO,         # UF_CRM_6531923E155B0 -> UF_CRM_6531923E155B0 (mesmo)
    LEAD_FIELD_CODIGO_AGENDAMENTO: DEAL_FIELD_CODIGO_AGENDAMENTO,   # UF_CRM_1729176843003 -> UF_CRM_6727964521E7A
    LEAD_FIELD_PROCEDIMENTO: DEAL_FIELD_PROCEDIMENTO,               # UF_CRM_1729180875 -> UF_CRM_672796455B831
    LEAD_FIELD_PROCEDIMENTO_NOME: DEAL_FIELD_PROCEDIMENTO_NOME,     # UF_CRM_1691074328 -> UF_CRM_1691074328 (mesmo)
    LEAD_FIELD_CODIGO_CLIENTE_BELLE: DEAL_FIELD_CODIGO_CLIENTE_BELLE,  # UF_CRM_1730401249284 -> UF_CRM_672796456F150
    LEAD_FIELD_ORIGEM: DEAL_FIELD_ORIGEM,                           # UF_CRM_1692640693814 -> UF_CRM_64E79DD16B294 (DIFERENTE!)
    LEAD_FIELD_CAMPANHA: DEAL_FIELD_CAMPANHA,                       # UF_CRM_1729176132205 -> UF_CRM_1720714625351 (DIFERENTE!)
    LEAD_FIELD_TIPO_ATENDIMENTO: DEAL_FIELD_TIPO_ATENDIMENTO,       # UF_CRM_1695303850778 -> UF_CRM_1695303850778 (mesmo)
    LEAD_FIELD_AGENDADOR: DEAL_FIELD_AGENDADOR,                     # UF_CRM_1729176345 -> UF_CRM_1697667153 (DIFERENTE!)
    LEAD_FIELD_TIPO_PACIENTE: DEAL_FIELD_TIPO_PACIENTE,             # UF_CRM_1729176219395 -> UF_CRM_65118CE0CE1BD (DIFERENTE!)
    LEAD_FIELD_SEGMENTO: DEAL_FIELD_SEGMENTO,                       # UF_CRM_1729176820 -> UF_CRM_67279644E834B
    LEAD_FIELD_EQUIPAMENTO: DEAL_FIELD_EQUIPAMENTO,                 # UF_CRM_1729176824131 -> UF_CRM_1729176824131 (mesmo)
}

# ==========================================
# MAPEAMENTO DE CÓDIGOS BELLE -> IDS BITRIX
# Converte códigos do sistema Belle para IDs de elementos no Bitrix
# ==========================================

# Profissionais: Código Belle -> ID Bitrix (IBLOCK 32)
BELLE_TO_BITRIX_PROFISSIONAL = {
    "103585": 8232,    # AMANDA RAFAELA FINKLER
    "39340": 1014,     # NADYA RIBEIRO
    "39898": 1000,     # LETYCIA OLIVEIRA
    "74361": 6636,     # MARIA APARECIDA ALCE DE SOUZA
    "88681": 7816,     # LUANA DA SILVA BARBOSA
    "100822": 7960,    # ANNY KAROLLINY
    "108195": 9038,    # TAIRANE DE SOUZA MORAES
    "EVELIN": 1002,    # EVELIM
    "KELLY": 268,      # DRA KELLY DA CAS
    "NATASHA": 266,    # DRA NATASHA
    "101754": 8234,    # EMILLY QUERINA PEGORARI (ajustar se necessário)
}

# Estabelecimentos: Código Belle -> ID Bitrix (IBLOCK 30)
BELLE_TO_BITRIX_ESTABELECIMENTO = {
    "1": 238,     # CLINICA CREPALDI DERMATO
    "2": 240,     # SPA CREPALDI
    "5": 242,     # CLINICA DERMATO E CONVENIOS LTDA
    "10": 244,    # DRIPS CLINIC
    "11": 246,    # CREPALDI CLINICA DE ESTETICA LTDA
    "12": 248,    # ESPAÇO BELA LASER
    "14": 8510,   # KLAYNE MOURA SERVIÇOS MEDICOS LTDA
}

# Segmentos: Código Belle -> ID Bitrix (IBLOCK 30) - mesmo que estabelecimentos
BELLE_TO_BITRIX_SEGMENTO = BELLE_TO_BITRIX_ESTABELECIMENTO

# Procedimentos: Nome (uppercase) -> ID Bitrix (IBLOCK 34)
# Usado para mapear o nome do serviço que vem do Belle para o ID no Bitrix
BITRIX_PROCEDIMENTO_POR_NOME = {
    "AVALIACAO DE PROCEDIMENTO": 7054,
    "CONSULTA": 7056,
    "MASSAGEM RELAXANTE": 7166,
    "MASSAGEM COM PEDRAS QUENTES": 7174,
    "DRENAGEM CORPO": 7178,
    "DRENAGEM ROSTO": 7182,
    "LIMPEZA DE PELE": 7146,
    "HECCUS": 7198,
    "ULTRAFORMER": 7328,
    "BOTOX": 7740,
    "ACIDO HIALURONICO": 7744,
    "SCULPTRA": 7780,
    "RADIESSE": 7800,
    "BLEFAROPLASTIA": 7784,
    "RINOMODELACAO": 7772,
    "FIO PDO": 7762,
    "DEPILACAO MOTUS": 7562,
    "MASSAGEM AROMATICA": 7874,
    "MASSAGEM DETOX": 7360,
    "MASSAGEM MODELADORA": 7262,
    "MASSAGEM COM PINDAS": 7298,
    "MASSAGEM ESPORTIVA": 7326,
    "MASSAGEM BIOENERGETICA": 7284,
    "MASSAGEM REVIGORANTE": 7370,
    "MASSAGEM NOS PÉS": 7428,
    "MASSAGEM BAMBUTERAPIA": 7952,
    "MASSAGEM CRANIO FACIL": 7618,
    "MASSAGEM ESFOLIANTE DETOX COM MANTA TERMICA": 7590,
    "QUICK MASSAGEM": 7264,
    "PEELING": 7238,
    "CARBOXITERAPIA": 7364,
    "ZFIELD": 7368,
    "POWER SHAPE": 7540,
    "LED": 7202,
    "EXILIS": 7116,
    "LEGACY": 7492,
    "EMFACE": 7630,
    "EMSCULPT NEO": 7752,
    "ULTRAFORMER III": 7748,
    "VOLNEWMER": 7866,
    "PICOSURE": 7758,
    "MORPHEUS - RADIOFREQUENCIA MICROAGULAHADA": 7774,
    "PROFHILO": 7778,
    "HARMONYCAS": 7782,
    "ELLEVA": 7796,
    "DAY SPA": 7420,
    "CONSULTA MEDICA CRM 6492": 7272,
    "CONSULTA MEDICA CRM 2425": 7332,
    "CONSULTA MEDICA CRM 11271": 7530,
    "CONSULTA 1ª VEZ": 8344,
    "RETORNO": 8724,
    "FACE SPA KOREANO": 8198,
    "SKIN BOOSTER": 8350,
    "SUSPENSÃO ELASTICA DA FACE": 8754,
}

# Aliases para manter compatibilidade com código existente
FIELD_DATA_AGENDAMENTO = LEAD_FIELD_DATA_AGENDAMENTO
FIELD_CODIGO_AGENDAMENTO = LEAD_FIELD_CODIGO_AGENDAMENTO
FIELD_PROFISSIONAL = LEAD_FIELD_PROFISSIONAL
FIELD_ESTABELECIMENTO = LEAD_FIELD_ESTABELECIMENTO
FIELD_PROCEDIMENTO = LEAD_FIELD_PROCEDIMENTO
FIELD_TIPO_CONSULTA = LEAD_FIELD_TIPO_ATENDIMENTO
FIELD_EQUIPAMENTO = LEAD_FIELD_EQUIPAMENTO  # string - Equipamento
FIELD_CODIGO_CLIENTE_BELLE = LEAD_FIELD_CODIGO_CLIENTE_BELLE

# ==========================================
# MAPEAMENTO DE ESTABELECIMENTOS -> PIPELINES
# ==========================================
# Cada estabelecimento pertence a um pipeline específico
# Mapeamento: código Belle do estabelecimento -> (CATEGORY_ID, STAGE_ID)

# Pipelines disponíveis:
# 42: Negócios - Clinica SPA
# 48: Negócios - Convenios
# 50: Negócios - Bela Laser
# 54: Negócios - Nutrologia

ESTABELECIMENTO_TO_PIPELINE = {
    # Clinica SPA (DRIPS, SPA, DERMATO, ESTETICA)
    "1": (42, "C42:UC_12PH7E"),    # CLINICA CREPALDI DERMATO
    "2": (42, "C42:UC_12PH7E"),    # SPA CREPALDI
    "10": (42, "C42:UC_12PH7E"),   # DRIPS CLINIC
    "11": (42, "C42:UC_12PH7E"),   # CREPALDI CLINICA DE ESTETICA LTDA
    # Convenios
    "5": (48, "C48:UC_7TGHGJ"),    # CLINICA DERMATO E CONVENIOS LTDA
    # Bela Laser
    "12": (50, "C50:UC_KEJS7X"),   # ESPAÇO BELA LASER
    # Nutrologia
    "14": (54, "C54:NEW"),         # KLAYNE MOURA SERVIÇOS MEDICOS LTDA
}

# Pipeline padrão caso o estabelecimento não esteja mapeado
DEAL_CATEGORY_ID_DEFAULT = 42
DEAL_STAGE_DEFAULT = "C42:UC_12PH7E"

# ==========================================
# MAPEAMENTO DE IDs DE ENUMERAÇÃO LEAD -> DEAL
# ==========================================
# Os campos de enumeração no Bitrix têm o MESMO field_id entre Lead e Deal,
# porém os IDs das OPÇÕES são DIFERENTES. Estes mapeamentos convertem
# o ID da opção do Lead para o ID correspondente no Deal.

# Mapeamento ORIGEM: Lead ID (UF_CRM_1692640693814) -> Deal ID (UF_CRM_64E79DD16B294)
LEAD_TO_DEAL_ENUM_ORIGEM = {
    "2138": "2136",    # Facebook Ads
    "136": "220",      # Google Ads
    "200": "222",      # Site
    "610": "614",      # Campanha
    "208": "3442",     # Indicação
    "3674": "3672",    # Instagram - Perfil Dra Kelly
    "3566": "3556",    # Instagram - Perfil Dra Natasha
    "3568": "3564",    # Instagram - Perfil Grupo Crepaldi
    "3570": "3558",    # Instagram - Perfil SPA
    "3572": "3562",    # Instagram - Perfil Convenios
    "3574": "3560",    # Instagram - Perfil Bela Laser
    "1788": "1784",    # Instragram Post
    "1790": "1786",    # Iniciativa do paciente
    "3612": "3610",    # Iniciativa Interna
    "7306": "7312",    # Agendamento Presencial
    "1016": "1014",    # Organico
    "7370": "7376",    # Remarketing SPA
    "7380": "7392",    # Remarketing Clinica Crepaldi
    "7382": "7394",    # SPA
    "634": "638",      # Não identificado
    "7400": "7406",    # Grupo OFF estetica
    "7746": "7752",    # Instragram - Perfil Dr. Paulo
    "8192": "8198",    # Agendamento por Ligação
    "8484": "8490",    # Parceria
}

# Mapeamento CAMPANHA: Lead ID (UF_CRM_1729176132205) -> Deal ID (UF_CRM_1720714625351)
LEAD_TO_DEAL_ENUM_CAMPANHA = {
    "7358": "7360",    # Não veio por campanha
    "3720": "3306",    # Depilação a Laser
    "3722": "7268",    # Ultraforme
    "3724": "7270",    # Quizena do Botox
    "3726": "7272",    # Vem verão Crepaldi
    "3728": "3620",    # Salamê Minguê Crepaldi
    "3730": "3682",    # You Inside The Box
    "7264": "7274",    # Plano Anual de Botox
    "7266": "7276",    # Blefaroplastia
    "7328": "7330",    # Day spa
    "7300": "7302",    # Day Spa de Aniversario
    "7316": "7318",    # Soft Lift
    "7322": "7324",    # Limpeza de Pele
    "7334": "7336",    # Elas no Campo
    "7340": "7342",    # Volnewmer
    "7346": "7348",    # Ultraforme III
    "7352": "7354",    # Power Shape
    "7364": "7366",    # Heccus
    "7410": "7412",    # Fotona
    "7416": "7426",    # Massagem Cranio Facial
    "7418": "7428",    # Massagem com Pindas
    "7420": "7430",    # Massagem Relaxante
    "7466": "7468",    # Ventosa
    "7422": "7432",    # Botox
    "7424": "7434",    # Geral
    "7472": "7474",    # Melasma
    "7478": "7480",    # Zfield
    "7688": "7690",    # Face Skin Koreano
    "7700": "7704",    # Drenagem
    "7702": "7706",    # Post Direcionando ao Whats
    "7712": "7714",    # Pure Skin Ritual
    "7718": "7720",    # Chikungunya
    "7734": "7738",    # Avaliação Gratuita
    "7736": "7740",    # Miofascial
    "7756": "7760",    # Procedimento - Dr Paulo
    "7758": "7762",    # Consulta - Dr Paulo
    "7768": "7774",    # Protocolo Alto em Colageno
    "7770": "7776",    # Protocolo Alto em Rejuvenescimento
    "7772": "7778",    # Protocolo Alto em Firmeza
    "7786": "7794",    # Protocolo Dia das Mães
    "7788": "7796",    # Remoção de Tatuagem
    "7790": "7798",    # Remarketing Blefaro
    "7792": "7800",    # Venquish
    "8026": "8028",    # Dia dos Namorados
    "8202": "8210",    # Acido Hialuronico
    "8204": "8212",    # Radiesse
    "8206": "8214",    # Tratamento Orelha Rasgada
    "8208": "8216",    # Rinomodelacao
    "8286": "8288",    # Suspensão Elastica - Cuiabá + Raio
    "8292": "8296",    # Suspensão Elastica - Cuiabá + Profissões
    "8294": "8298",    # Suspensão Elastica - Outras Cidades
    "8332": "8334",    # Avaliação Gratuita - Lipedema
    "8348": "8350",    # Naturalidade
    "8472": "8474",    # Ultraformer Face Pescoço - Black Friday
    "8494": "8496",    # DEVILLE HOTEIS E TURISMO LTDA.
    "8500": "8502",    # Depilação - Black Friday
    "8524": "8526",    # Cartão Presente
    "9626": "9628",    # Bumbum
    "10124": "10126",  # Day Spa Mês de Mulher
}

# Mapeamento TIPO ATENDIMENTO: Lead ID (UF_CRM_1695303850778) -> Deal ID (UF_CRM_650C5648BE36D)
LEAD_TO_DEAL_ENUM_TIPO_ATENDIMENTO = {
    "718": "742",      # Venda de Plano
    "3712": "3710",    # Renovação de Plano
    "1642": "1634",    # Consulta
    "1638": "1632",    # Avaliação Gratuita
    "1640": "1636",    # Avaliação Paga
    "1846": "1844",    # Utilização de Voucher
    "720": "744",      # Baixa de Sessão
    "722": "746",      # Retorno
    "726": "750",      # Cortesia
    "3388": "3386",    # Permuta
    "7244": "7250",    # Venda de Voucher
    "728": "752",      # Lead Desqualificado
    "8338": "8344",    # Lista de Espera - Nutrologia
}

# Mapeamento direto de valores de texto para IDs do Deal (UF_CRM_650C5648BE36D)
# Usado quando o workflow envia o tipo de atendimento como texto
TIPO_ATENDIMENTO_TEXTO_PARA_ID = {
    "VENDA DE PLANO": "742",
    "RENOVACAO DE PLANO": "3710",
    "RENOVAÇÃO DE PLANO": "3710",
    "CONSULTA": "1634",
    "AVALIACAO GRATUITA": "1632",
    "AVALIAÇÃO GRATUITA": "1632",
    "AVALIACAO PAGA": "1636",
    "AVALIAÇÃO PAGA": "1636",
    "UTILIZACAO DE VOUCHER": "1844",
    "UTILIZAÇÃO DE VOUCHER": "1844",
    "BAIXA DE SESSAO": "744",
    "BAIXA DE SESSÃO": "744",
    "RETORNO": "746",
    "CORTESIA": "750",
    "PERMUTA": "3386",
    "VENDA DE VOUCHER": "7250",
    "LEAD DESQUALIFICADO": "752",
    "LISTA DE ESPERA - NUTROLOGIA": "8344",
    # Também aceita apenas o ID do Deal diretamente
    "742": "742",
    "3710": "3710",
    "1634": "1634",
    "1632": "1632",
    "1636": "1636",
    "1844": "1844",
    "744": "744",
    "746": "746",
    "750": "750",
    "3386": "3386",
    "7250": "7250",
    "752": "752",
    "8344": "8344",
}

# Mapeamento TIPO PACIENTE: Lead ID (UF_CRM_1729176219395) -> Deal ID (UF_CRM_65118CE0CE1BD)
LEAD_TO_DEAL_ENUM_TIPO_PACIENTE = {
    "3732": "7032",    # Paciente Novo
    "3734": "7034",    # Paciente
    "3736": "2920",    # Recall
}

# Dicionário que mapeia cada field_id de enumeração para seu mapeamento correspondente
ENUM_FIELD_MAPPINGS = {
    "UF_CRM_1692640693814": LEAD_TO_DEAL_ENUM_ORIGEM,          # Origem
    "UF_CRM_1729176132205": LEAD_TO_DEAL_ENUM_CAMPANHA,        # Campanha
    "UF_CRM_1695303850778": LEAD_TO_DEAL_ENUM_TIPO_ATENDIMENTO,  # Tipo Atendimento
    "UF_CRM_1729176219395": LEAD_TO_DEAL_ENUM_TIPO_PACIENTE,   # Tipo Paciente
}

# Lista de campos que são do tipo enumeração e precisam de conversão
ENUM_FIELDS = set(ENUM_FIELD_MAPPINGS.keys())


def converter_belle_para_bitrix_profissional(codigo_belle: str) -> int | None:
    """
    Converte código de profissional do Belle para ID do Bitrix (IBLOCK 32).

    Args:
        codigo_belle: Código do profissional no sistema Belle

    Returns:
        ID do elemento no Bitrix, ou None se não encontrar
    """
    codigo_str = str(codigo_belle).strip()
    bitrix_id = BELLE_TO_BITRIX_PROFISSIONAL.get(codigo_str)
    if bitrix_id:
        logger.info("profissional_convertido_belle_bitrix", codigo_belle=codigo_str, bitrix_id=bitrix_id)
    else:
        logger.warning("profissional_sem_mapeamento_belle_bitrix", codigo_belle=codigo_str)
    return bitrix_id


def converter_belle_para_bitrix_estabelecimento(codigo_belle: str) -> int | None:
    """
    Converte código de estabelecimento do Belle para ID do Bitrix (IBLOCK 30).

    Args:
        codigo_belle: Código do estabelecimento no sistema Belle

    Returns:
        ID do elemento no Bitrix, ou None se não encontrar
    """
    codigo_str = str(codigo_belle).strip()
    bitrix_id = BELLE_TO_BITRIX_ESTABELECIMENTO.get(codigo_str)
    if bitrix_id:
        logger.info("estabelecimento_convertido_belle_bitrix", codigo_belle=codigo_str, bitrix_id=bitrix_id)
    else:
        logger.warning("estabelecimento_sem_mapeamento_belle_bitrix", codigo_belle=codigo_str)
    return bitrix_id


def obter_pipeline_por_estabelecimento(codigo_estabelecimento: str) -> tuple[int, str]:
    """
    Obtém o pipeline (CATEGORY_ID) e estágio (STAGE_ID) baseado no estabelecimento.

    Mapeamento:
    - Clinica SPA (42): DRIPS, SPA, DERMATO, ESTETICA (códigos 1, 2, 10, 11)
    - Convenios (48): Convenios (código 5)
    - Bela Laser (50): Bela Laser (código 12)
    - Nutrologia (54): Klayne Moura (código 14)

    Args:
        codigo_estabelecimento: Código Belle do estabelecimento

    Returns:
        Tupla (CATEGORY_ID, STAGE_ID) do pipeline correspondente
    """
    codigo_str = str(codigo_estabelecimento).strip()
    pipeline_info = ESTABELECIMENTO_TO_PIPELINE.get(codigo_str)

    if pipeline_info:
        category_id, stage_id = pipeline_info
        logger.info(
            "pipeline_selecionado_por_estabelecimento",
            estabelecimento=codigo_str,
            category_id=category_id,
            stage_id=stage_id
        )
        return pipeline_info
    else:
        logger.warning(
            "estabelecimento_sem_pipeline_mapeado_usando_padrao",
            estabelecimento=codigo_str,
            category_id_padrao=DEAL_CATEGORY_ID_DEFAULT,
            stage_id_padrao=DEAL_STAGE_DEFAULT
        )
        return (DEAL_CATEGORY_ID_DEFAULT, DEAL_STAGE_DEFAULT)


def converter_belle_para_bitrix_segmento(codigo_belle: str) -> int | None:
    """
    Converte código de segmento do Belle para ID do Bitrix (IBLOCK 30).

    Args:
        codigo_belle: Código do segmento no sistema Belle

    Returns:
        ID do elemento no Bitrix, ou None se não encontrar
    """
    codigo_str = str(codigo_belle).strip()
    bitrix_id = BELLE_TO_BITRIX_SEGMENTO.get(codigo_str)
    if bitrix_id:
        logger.info("segmento_convertido_belle_bitrix", codigo_belle=codigo_str, bitrix_id=bitrix_id)
    return bitrix_id


def extrair_nome_procedimento(procedimento_str: str) -> str | None:
    """
    Extrai o nome do procedimento de strings no formato:
    - "servico[ID][nome]=NOME DO PROCEDIMENTO"
    - ou apenas "NOME DO PROCEDIMENTO"

    Args:
        procedimento_str: String contendo o procedimento

    Returns:
        Nome do procedimento em uppercase, ou None se não encontrar
    """
    if not procedimento_str:
        return None

    # Tenta extrair do formato servico[ID][nome]=NOME
    if "[nome]=" in procedimento_str.lower():
        # Pega tudo após o último "="
        partes = procedimento_str.split("=")
        if len(partes) >= 2:
            nome = partes[-1].strip().upper()
            return nome

    # Se não tem o formato especial, usa o valor direto
    return procedimento_str.strip().upper()


def converter_procedimento_para_bitrix(procedimento_str: str) -> int | None:
    """
    Converte nome de procedimento para ID do Bitrix (IBLOCK 34).

    Args:
        procedimento_str: String contendo o procedimento (pode ser no formato
                         "servico[ID][nome]=NOME" ou apenas "NOME")

    Returns:
        ID do elemento no Bitrix, ou None se não encontrar
    """
    nome = extrair_nome_procedimento(procedimento_str)
    if not nome:
        logger.warning("procedimento_nome_vazio", valor_original=procedimento_str)
        return None

    bitrix_id = BITRIX_PROCEDIMENTO_POR_NOME.get(nome)
    if bitrix_id:
        logger.info("procedimento_convertido_bitrix", nome=nome, bitrix_id=bitrix_id)
    else:
        logger.warning("procedimento_sem_mapeamento_bitrix", nome=nome, valor_original=procedimento_str)
    return bitrix_id


def converter_enum_lead_para_deal(field_id: str, lead_value: str | int) -> str | int:
    """
    Converte um valor de enumeração do Lead para o ID correspondente no Deal.

    Os campos de enumeração no Bitrix têm os mesmos field_ids entre Lead e Deal,
    mas os IDs das opções são diferentes. Esta função mapeia o valor do Lead
    para o valor equivalente no Deal.

    Args:
        field_id: ID do campo (ex: UF_CRM_1692640693814)
        lead_value: Valor do campo no Lead (ID da opção)

    Returns:
        ID da opção correspondente no Deal, ou o valor original se não encontrar mapeamento.
    """
    if not lead_value:
        return lead_value

    # Verifica se é um campo de enumeração que precisa conversão
    if field_id not in ENUM_FIELD_MAPPINGS:
        return lead_value

    mapping = ENUM_FIELD_MAPPINGS[field_id]
    lead_value_str = str(lead_value)

    if lead_value_str in mapping:
        deal_value = mapping[lead_value_str]
        logger.info(
            "enum_convertido_lead_deal",
            field_id=field_id,
            lead_value=lead_value_str,
            deal_value=deal_value
        )
        return deal_value

    # Se não encontrou mapeamento, loga aviso e retorna original
    logger.warning(
        "enum_sem_mapeamento",
        field_id=field_id,
        lead_value=lead_value_str,
        msg="Valor de enumeração do Lead não tem correspondente no Deal"
    )
    return lead_value


# Mapeamento de ID interno do Bitrix para código Belle
# O Bitrix envia o ID interno do elemento na lista, não o campo "ID" (código Belle)
# Esse mapeamento converte: bitrix_interno_id -> belle_code
BITRIX_TO_BELLE_ESTABELECIMENTO = {
    238: 1,    # CLINICA CREPALDI DERMATO
    240: 2,    # SPA CREPALDI
    242: 5,    # CLINICA DERMATO E CONVENIOS LTDA
    244: 10,   # DRIPS CLINIC
    246: 11,   # CREPALDI CLINICA DE ESTETICA LTDA
    248: 12,   # ESPAÇO BELA LASER
    8510: 14,  # KLAYNE MOURA SERVIÇOS MEDICOS LTDA
}

# Lista de códigos Belle válidos (usados quando o código já vem correto)
BELLE_CODES_VALIDOS = {1, 2, 5, 10, 11, 12, 14}

# Mapeamento de código Belle para nome do estabelecimento
BELLE_ESTABELECIMENTO_NOMES = {
    1: "Clínica Crepaldi Dermato",
    2: "Spa Crepaldi",
    5: "Clínica Dermato e Convênios",
    10: "Drips Clinic",
    11: "Crepaldi Clínica de Estética",
    12: "Espaço Bela Laser",
    14: "Klayne Moura Serviços Médicos",
}

# Mapeamento estático de profissionais (código -> nome)
# Atualizado manualmente com base nos profissionais cadastrados na Belle
BELLE_PROFISSIONAIS = {
    "100822": "Anny Karolliny",
    "103585": "Amanda Rafaela Finkler",
    "108195": "Tairane de Souza Moraes",
    "39340": "Nadya Ribeiro",
    "39898": "Letycia Oliveira",
    "74361": "Maria Aparecida",
    "88681": "Luana Barbosa",
    "EVELIN": "Evelim",
    "KELLY": "Dra Kelly da Cas",
    "NATASHA": "Dra Natasha",
}


def parse_bitrix_servico(procedimento: str, query_params: dict = None) -> dict:
    """
    Parseia o formato de serviço do Bitrix para extrair código, nome e tempo.

    O Bitrix envia no formato:
      procedimento=servico[56150858][nome]=MASSAGEM AROMATICA
      servico[56150858][tempo]=60

    Retorna dict com:
      - codServico: código do serviço (ex: "56150858")
      - nomeServico: nome do serviço (ex: "MASSAGEM AROMATICA")
      - tempo: duração em minutos (ex: 60)
    """
    result = {
        "codServico": None,
        "nomeServico": None,
        "tempo": 30,  # Default
    }

    if not procedimento:
        return result

    # Tenta extrair código do formato servico[CODE][nome]=NAME
    # Padrão: servico[12345][nome]=Nome do Servico
    match = re.search(r'servico\[(\d+)\]\[nome\]=(.+)', procedimento, re.IGNORECASE)
    if match:
        result["codServico"] = match.group(1)
        result["nomeServico"] = match.group(2).strip()

        # Busca o tempo nos query_params
        if query_params:
            tempo_key = f"servico[{result['codServico']}][tempo]"
            if tempo_key in query_params:
                try:
                    result["tempo"] = int(query_params[tempo_key])
                except (ValueError, TypeError):
                    pass

        logger.info("servico_parseado_formato_bitrix", **result)
        return result

    # Se não é o formato servico[], tenta usar como código direto
    # Pode ser um número separado por vírgula ou um código simples
    servicos = [s.strip() for s in procedimento.split(",") if s.strip()]
    if servicos:
        primeiro = servicos[0]
        # Verifica se é um número
        if primeiro.isdigit():
            result["codServico"] = primeiro
            result["nomeServico"] = primeiro
        else:
            # Usa como está (pode ser nome ou código alfanumérico)
            result["codServico"] = primeiro
            result["nomeServico"] = primeiro

    logger.info("servico_parseado_formato_simples", **result)
    return result


class AgendamentoRequest(BaseModel):
    """Dados recebidos do workflow Bitrix."""

    # Dados do Lead
    lead_id: int
    lead_nome: str | None = None
    lead_telefone: str | None = None
    codigo_cliente_belle: str | None = None

    # Dados do Agendamento
    data_agendamento: str  # formato: dd/mm/yyyy
    horario: str  # formato: HH:MM
    estabelecimento_codigo: int
    estabelecimento_nome: str | None = None
    profissional_codigo: int
    profissional_nome: str | None = None
    tipo_agendamento: str
    servicos: str  # lista separada por vírgula
    tempo: int = 15
    equipamento_codigo: int | None = None
    equipamento_nome: str | None = None
    novo_card: bool = False
    observacao: str = ""
    agendador: str | None = None  # ID do usuário que executou o workflow


class AgendamentoResponse(BaseModel):
    """Resposta do webhook."""

    success: bool
    message: str
    codigo_agendamento: str | None = None
    lead_id: int | None = None
    warning: str | None = None


def bitrix_call(method: str, params: dict | None = None) -> dict[str, Any]:
    """Faz chamada à API do Bitrix."""
    url = f"{BITRIX_WEBHOOK_URL}/{method}"
    try:
        response = httpx.post(url, json=params or {}, timeout=30.0)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        logger.error("bitrix_api_error", method=method, error=str(e))
        raise


def adicionar_produto_ao_deal(deal_id: int, produto_nome: str, preco: float = 0, quantidade: int = 1) -> bool:
    """
    Adiciona um produto à aba Orçamento do Deal.

    Args:
        deal_id: ID do Deal no Bitrix
        produto_nome: Nome do produto/serviço
        preco: Preço unitário (default 0)
        quantidade: Quantidade (default 1)

    Returns:
        True se sucesso, False se erro
    """
    try:
        produto_row = {
            "PRODUCT_NAME": produto_nome,
            "PRICE": preco,
            "QUANTITY": quantidade,
        }

        response = bitrix_call("crm.deal.productrows.set", {
            "id": deal_id,
            "rows": [produto_row]
        })

        if response.get("result"):
            logger.info(
                "produto_adicionado_deal",
                deal_id=deal_id,
                produto=produto_nome,
                preco=preco
            )
            return True
        else:
            logger.warning(
                "erro_adicionar_produto_deal",
                deal_id=deal_id,
                produto=produto_nome,
                response=response
            )
            return False

    except Exception as e:
        logger.error(
            "excecao_adicionar_produto_deal",
            deal_id=deal_id,
            produto=produto_nome,
            error=str(e)
        )
        return False


def belle_call(endpoint: str, payload: dict, method: str = "POST") -> dict[str, Any]:
    """Faz chamada à API da Belle Software."""
    url = f"{BELLE_BASE_URL}{endpoint}"
    headers = {
        "Authorization": BELLE_TOKEN,
        "Content-Type": "application/json",
    }
    try:
        logger.info("belle_api_request", url=url, method=method, payload=payload)
        if method == "GET":
            response = httpx.get(url, params=payload, headers=headers, timeout=60.0)
        else:
            response = httpx.post(url, json=payload, headers=headers, timeout=60.0)
        response.raise_for_status()
        result = response.json()
        logger.info("belle_api_response", response=result)
        return result
    except httpx.HTTPError as e:
        response_text = None
        if hasattr(e, 'response') and e.response is not None:
            response_text = e.response.text
        logger.error("belle_api_error", endpoint=endpoint, error=str(e), response_text=response_text)
        raise


def gerar_cpf_valido(seed: int) -> str:
    """
    Gera um CPF válido baseado em uma seed (ex: lead_id).
    Usado quando o lead não tem CPF cadastrado no Bitrix.
    """
    # Usa a seed para gerar os 9 primeiros dígitos
    base = str(seed).zfill(9)[-9:]  # Pega os últimos 9 dígitos

    # Calcula primeiro dígito verificador
    soma = sum(int(base[i]) * (10 - i) for i in range(9))
    resto = soma % 11
    d1 = 0 if resto < 2 else 11 - resto

    # Calcula segundo dígito verificador
    base_com_d1 = base + str(d1)
    soma = sum(int(base_com_d1[i]) * (11 - i) for i in range(10))
    resto = soma % 11
    d2 = 0 if resto < 2 else 11 - resto

    return base + str(d1) + str(d2)


def criar_cliente_belle(nome: str, telefone: str, codEstab: int, cpf: str = None, email: str = None, lead_id: int = None) -> dict[str, Any]:
    """
    Cria um cliente na Belle Software.
    Se não tiver CPF, gera um CPF válido baseado no lead_id.

    Retorna dict com 'codigo' do cliente criado.
    """
    payload = {
        "nome": nome,
        "codEstab": codEstab,
    }

    # Limpa e formata telefone (remove espaços e caracteres especiais)
    if telefone:
        telefone_limpo = "".join(c for c in telefone if c.isdigit())
        # Adiciona DDD se necessário
        if len(telefone_limpo) >= 10:
            payload["celular"] = telefone_limpo
            payload["ddiCelular"] = "+55"

    # CPF é obrigatório na API Belle
    if cpf:
        cpf_limpo = "".join(c for c in cpf if c.isdigit())
        if len(cpf_limpo) == 11:
            payload["cpf"] = cpf_limpo
        else:
            # CPF inválido, gera um baseado no lead_id
            payload["cpf"] = gerar_cpf_valido(lead_id or 999999999)
    else:
        # Sem CPF, gera um baseado no lead_id
        payload["cpf"] = gerar_cpf_valido(lead_id or 999999999)

    if email:
        payload["email"] = email

    logger.info("criando_cliente_belle", payload=payload)
    # Endpoint de cliente requer CPF
    return belle_call("/api/release/controller/IntegracaoExterna/v1.0/cliente/gravar", payload)


def buscar_nome_profissional(cod_profissional: str) -> str:
    """
    Busca o nome do profissional pelo código usando mapeamento estático.

    Args:
        cod_profissional: Código do profissional

    Returns:
        Nome do profissional ou o código se não encontrar.
    """
    if not cod_profissional:
        return "Profissional"

    # Busca no mapeamento estático
    nome = BELLE_PROFISSIONAIS.get(str(cod_profissional))
    if nome:
        return nome

    # Retorna o código se não encontrou no mapeamento
    return str(cod_profissional)


def validar_profissional_no_estabelecimento(cod_profissional: str, cod_estab: int) -> tuple[bool, str | None, str | None]:
    """
    Valida se o profissional pertence ao estabelecimento usando a API Belle.

    Args:
        cod_profissional: Código do profissional
        cod_estab: Código do estabelecimento Belle

    Returns:
        Tupla com (valido, nome_profissional, mensagem_erro)
        - valido: True se o profissional pertence ao estabelecimento
        - nome_profissional: Nome do profissional se encontrado
        - mensagem_erro: Mensagem de erro se não for válido
    """
    if not cod_profissional:
        return False, None, "Código do profissional não informado"

    try:
        # Busca usuários do estabelecimento na API Belle
        endpoint = "/api/release/controller/IntegracaoExterna/v1.0/usuario/buscar"
        payload = {"codEstab": cod_estab}

        logger.info(
            "validando_profissional_estabelecimento",
            cod_profissional=cod_profissional,
            cod_estab=cod_estab
        )

        response = belle_call(endpoint, payload, method="GET")

        # Procura o profissional na lista de usuários
        usuarios = response.get("usuarios", response.get("result", []))

        if not usuarios:
            logger.warning(
                "nenhum_usuario_encontrado_no_estabelecimento",
                cod_estab=cod_estab,
                response=response
            )
            # Se não conseguiu buscar usuários, permite o agendamento mas com aviso
            return True, None, None

        # Normaliza código para string para comparação
        cod_profissional_str = str(cod_profissional).strip().upper()

        for usuario in usuarios:
            cod_usuario = str(usuario.get("cod_usuario", "")).strip().upper()
            if cod_usuario == cod_profissional_str:
                nome = usuario.get("nom_usuario", "")
                possui_agenda = usuario.get("possui_agenda", "Não")

                logger.info(
                    "profissional_encontrado_no_estabelecimento",
                    cod_profissional=cod_profissional,
                    cod_estab=cod_estab,
                    nome=nome,
                    possui_agenda=possui_agenda
                )

                # Verifica se o profissional possui agenda
                if possui_agenda != "Sim":
                    return False, nome, f"Profissional {nome} ({cod_profissional}) não possui agenda habilitada no estabelecimento"

                return True, nome, None

        # Profissional não encontrado no estabelecimento
        nomes_disponiveis = [u.get("nom_usuario", u.get("cod_usuario", "")) for u in usuarios if u.get("possui_agenda") == "Sim"][:5]
        nome_estab = BELLE_ESTABELECIMENTO_NOMES.get(cod_estab, f"Estabelecimento {cod_estab}")

        logger.warning(
            "profissional_nao_pertence_ao_estabelecimento",
            cod_profissional=cod_profissional,
            cod_estab=cod_estab,
            nome_estab=nome_estab,
            profissionais_disponiveis=nomes_disponiveis
        )

        return False, None, f"Profissional {cod_profissional} não pertence ao {nome_estab}. Profissionais disponíveis: {', '.join(nomes_disponiveis)}"

    except Exception as e:
        logger.error(
            "erro_validar_profissional_estabelecimento",
            cod_profissional=cod_profissional,
            cod_estab=cod_estab,
            error=str(e)
        )
        # Em caso de erro na validação, permite o agendamento mas loga o erro
        return True, None, None


def buscar_codigo_cliente_belle_no_contato(lead_id: int) -> str | None:
    """
    Busca o código do cliente Belle no contato associado ao lead.

    O lead pode ter um contato vinculado (CONTACT_ID) que já possui
    o código do cliente Belle no campo UF_CRM_1693232565211.

    Returns:
        Código do cliente Belle ou None se não encontrar.
    """
    try:
        # 1. Busca o lead para obter o CONTACT_ID
        logger.info("buscando_lead_para_contato", lead_id=lead_id)
        lead_response = bitrix_call("crm.lead.get", {"id": lead_id})
        if not lead_response or not lead_response.get("result"):
            logger.warning("lead_nao_encontrado", lead_id=lead_id, response=lead_response)
            return None

        lead_data = lead_response["result"]
        contact_id = lead_data.get("CONTACT_ID")

        logger.info(
            "lead_dados",
            lead_id=lead_id,
            contact_id=contact_id,
            lead_title=lead_data.get("TITLE"),
            lead_name=lead_data.get("NAME")
        )

        if not contact_id:
            logger.info("lead_sem_contato", lead_id=lead_id)
            return None

        # 2. Busca o contato para obter o código cliente Belle
        logger.info("buscando_contato", contact_id=contact_id)
        contact_response = bitrix_call("crm.contact.get", {"id": contact_id})
        if not contact_response or not contact_response.get("result"):
            logger.warning("contato_nao_encontrado", contact_id=contact_id, response=contact_response)
            return None

        contact_data = contact_response["result"]

        # Log de todos os campos UF_ do contato para debug
        campos_uf = {k: v for k, v in contact_data.items() if k.startswith("UF_")}
        logger.info(
            "contato_campos_uf",
            contact_id=contact_id,
            contact_name=contact_data.get("NAME"),
            campos_uf=campos_uf,
            campo_belle_esperado=FIELD_CODIGO_CLIENTE_BELLE_CONTATO
        )

        codigo_belle = contact_data.get(FIELD_CODIGO_CLIENTE_BELLE_CONTATO)

        if codigo_belle:
            logger.info(
                "codigo_cliente_belle_encontrado_no_contato",
                lead_id=lead_id,
                contact_id=contact_id,
                codigo_belle=codigo_belle,
                campo_usado=FIELD_CODIGO_CLIENTE_BELLE_CONTATO
            )
            return str(codigo_belle)

        logger.warning(
            "codigo_belle_nao_encontrado_no_contato",
            lead_id=lead_id,
            contact_id=contact_id,
            campo_esperado=FIELD_CODIGO_CLIENTE_BELLE_CONTATO,
            campos_disponiveis=list(campos_uf.keys())
        )
        return None

    except Exception as e:
        logger.error("erro_buscar_codigo_cliente_contato", lead_id=lead_id, error=str(e), exc_info=True)
        return None


def criar_agendamento_belle(
    codCliente: int,
    codServico: str,
    codEstab: int,
    data: str,
    hora: str,
    codProfissional: str = None,
    observacao: str = None,
    tempo: int = 30
) -> dict[str, Any]:
    """
    Cria um agendamento de serviço na Belle Software.
    Usa o formato documentado na API da Belle.

    IMPORTANTE: O campo 'tempo' DEVE ir dentro de cada serviço no array serv.
    """
    # Monta array de serviços no formato da API Belle
    # O campo tempo é OBRIGATÓRIO dentro de cada serviço
    serv_array = []
    if codServico:
        serv_array.append({
            "codServico": str(codServico),
            "nomeServico": str(codServico),
            "tempo": tempo,  # OBRIGATÓRIO - duração em minutos
        })

    # Payload no formato oficial da API Belle (Postman)
    # Enviando tanto "obs" quanto "observacao" para garantir compatibilidade
    payload = {
        "codCli": codCliente,  # Código do cliente
        "codEstab": codEstab,  # Código do estabelecimento
        "prof": {
            "cod_usuario": str(codProfissional) if codProfissional else "",
            "nom_usuario": "",
        },
        "dtAgd": data,  # Data no formato dd/mm/yyyy
        "hri": hora,  # Horário no formato HH:MM
        "serv": serv_array,  # Array de serviços
        "codPlano": "",
        "agSala": False,
        "codSala": 0,
        "codVendedor": "",
        "codEquipamento": None,
        "obs": observacao or "",  # Campo abreviado
        "observacao": observacao or "",  # Campo completo
    }

    logger.info("payload_belle_observacao", observacao=observacao, obs=observacao)

    logger.info("criando_agendamento_belle", payload=payload)
    # Endpoint documentado da API Belle para gravar agenda
    response = belle_call("/api/release/controller/IntegracaoExterna/v1.0/agenda/gravar", payload)

    # Verifica se a Belle retornou erro na resposta
    if response.get("msg") and not response.get("codAgendamento"):
        raise Exception(f"Belle API: {response.get('msg')}")

    return response


def converter_estabelecimento_para_belle(estabelecimento_id: int) -> int:
    """
    Converte o ID interno do Bitrix para o código Belle do estabelecimento.

    O Bitrix envia o ID interno do elemento na lista de Estabelecimentos,
    mas a Belle precisa do código do campo "ID" dessa lista.
    """
    # Se já é um código Belle válido, retorna direto
    if estabelecimento_id in BELLE_CODES_VALIDOS:
        logger.info("estabelecimento_ja_belle", code=estabelecimento_id)
        return estabelecimento_id

    # Verifica se tem mapeamento estático
    if estabelecimento_id in BITRIX_TO_BELLE_ESTABELECIMENTO:
        belle_code = BITRIX_TO_BELLE_ESTABELECIMENTO[estabelecimento_id]
        logger.info("estabelecimento_mapeado", bitrix_id=estabelecimento_id, belle_code=belle_code)
        return belle_code

    # Tenta buscar via API do Bitrix
    try:
        result = bitrix_call(
            "lists.element.get",
            {
                "IBLOCK_TYPE_ID": "lists",
                "IBLOCK_ID": 30,  # ID do bloco de informação "Estabelecimento"
                "ELEMENT_ID": estabelecimento_id,
            }
        )

        if result and result.get("result"):
            elementos = result["result"]
            if elementos:
                elemento = elementos[0] if isinstance(elementos, list) else elementos
                # O campo ID da lista está em PROPERTY_xxx ou direto
                # Tenta encontrar o campo ID
                belle_code = elemento.get("ID") or elemento.get("PROPERTY_ID")
                if belle_code:
                    logger.info(
                        "estabelecimento_obtido_api",
                        bitrix_id=estabelecimento_id,
                        belle_code=belle_code
                    )
                    return int(belle_code)
    except Exception as e:
        logger.warning(
            "erro_buscar_estabelecimento_api",
            bitrix_id=estabelecimento_id,
            error=str(e)
        )

    # Se não conseguiu converter, retorna o ID original com log de aviso
    logger.warning(
        "estabelecimento_nao_mapeado",
        bitrix_id=estabelecimento_id,
        msg="Usando ID original - adicione ao mapeamento BITRIX_TO_BELLE_ESTABELECIMENTO"
    )
    return estabelecimento_id


def adicionar_comentario_lead(lead_id: int, comentario: str) -> bool:
    """Adiciona um comentário na timeline do lead."""
    try:
        result = bitrix_call(
            "crm.timeline.comment.add",
            {
                "fields": {
                    "ENTITY_ID": lead_id,
                    "ENTITY_TYPE": "lead",
                    "COMMENT": comentario,
                }
            }
        )
        return result.get("result", False)
    except Exception as e:
        logger.error("erro_adicionar_comentario", lead_id=lead_id, error=str(e))
        return False


def atualizar_lead(lead_id: int, campos: dict) -> bool:
    """Atualiza campos do lead no Bitrix."""
    try:
        result = bitrix_call(
            "crm.lead.update",
            {
                "id": lead_id,
                "fields": campos,
            }
        )
        return result.get("result", False)
    except Exception as e:
        logger.error("erro_atualizar_lead", lead_id=lead_id, error=str(e))
        return False


def converter_lead_para_negocio(lead_id: int, codigo_agendamento: str = None, dados_agendamento: dict = None) -> dict:
    """
    Cria um negócio a partir do lead usando crm.deal.add.

    O negócio é criado na etapa "Agendados" do pipeline configurado.
    Copia todos os campos relevantes do lead para o negócio usando
    o mapeamento LEAD_TO_DEAL_FIELD_MAP.

    Args:
        lead_id: ID do lead
        codigo_agendamento: Código do agendamento Belle
        dados_agendamento: Dados extras do agendamento (profissional, procedimento, etc)

    Returns:
        dict com 'success', 'deal_id' e 'contact_id'
    """
    try:
        # Primeiro, obtém os dados do lead para copiar para o negócio
        lead_data = None
        contact_id = None
        try:
            lead_response = bitrix_call("crm.lead.get", {"id": lead_id})
            if lead_response and lead_response.get("result"):
                lead_data = lead_response["result"]
                contact_id = lead_data.get("CONTACT_ID")
        except Exception as e:
            logger.warning("erro_buscar_lead_para_conversao", lead_id=lead_id, error=str(e))

        # Usa o TITLE do lead como título do negócio (mantém o mesmo nome)
        titulo = "Novo Negócio"
        if lead_data:
            titulo = lead_data.get("TITLE") or lead_data.get("NAME") or titulo

        # Determina o pipeline baseado no estabelecimento
        estabelecimento_codigo = None
        if dados_agendamento and dados_agendamento.get("estabelecimento"):
            estabelecimento_codigo = str(dados_agendamento["estabelecimento"])

        category_id, stage_id = obter_pipeline_por_estabelecimento(estabelecimento_codigo)

        # Cria o negócio usando crm.deal.add
        deal_fields = {
            "TITLE": titulo,
            "CATEGORY_ID": category_id,
            "STAGE_ID": stage_id,
            "LEAD_ID": lead_id,
        }

        # Vincula o contato se existir
        if contact_id:
            deal_fields["CONTACT_ID"] = contact_id

        # Copia dados do lead para o negócio
        if lead_data:
            # Campos padrão do Bitrix
            if lead_data.get("ASSIGNED_BY_ID"):
                deal_fields["ASSIGNED_BY_ID"] = lead_data["ASSIGNED_BY_ID"]
            if lead_data.get("SOURCE_ID"):
                deal_fields["SOURCE_ID"] = lead_data["SOURCE_ID"]
            if lead_data.get("SOURCE_DESCRIPTION"):
                deal_fields["SOURCE_DESCRIPTION"] = lead_data["SOURCE_DESCRIPTION"]
            if lead_data.get("UTM_SOURCE"):
                deal_fields["UTM_SOURCE"] = lead_data["UTM_SOURCE"]
            if lead_data.get("UTM_MEDIUM"):
                deal_fields["UTM_MEDIUM"] = lead_data["UTM_MEDIUM"]
            if lead_data.get("UTM_CAMPAIGN"):
                deal_fields["UTM_CAMPAIGN"] = lead_data["UTM_CAMPAIGN"]
            if lead_data.get("UTM_CONTENT"):
                deal_fields["UTM_CONTENT"] = lead_data["UTM_CONTENT"]
            if lead_data.get("UTM_TERM"):
                deal_fields["UTM_TERM"] = lead_data["UTM_TERM"]

            # Copia campos personalizados usando o mapeamento Lead -> Deal
            # Os campos no Lead e Deal têm IDs diferentes, então usamos o mapa
            # IMPORTANTE: Campos de enumeração têm IDs de opções diferentes entre Lead e Deal,
            # então precisamos converter os valores usando ENUM_FIELD_MAPPINGS
            campos_copiados = []
            campos_vazios = []
            campos_convertidos = []

            for lead_field, deal_field in LEAD_TO_DEAL_FIELD_MAP.items():
                value = lead_data.get(lead_field)
                if value:
                    # Verifica se é um campo de enumeração que precisa conversão
                    if lead_field in ENUM_FIELDS:
                        original_value = value
                        value = converter_enum_lead_para_deal(lead_field, value)
                        if str(value) != str(original_value):
                            campos_convertidos.append({
                                "field": lead_field,
                                "original": original_value,
                                "converted": value
                            })

                    deal_fields[deal_field] = value
                    campos_copiados.append(lead_field)
                    logger.info(
                        "campo_copiado_lead_deal",
                        lead_field=lead_field,
                        deal_field=deal_field,
                        value=str(value)[:100],  # Trunca valor longo
                        is_enum=lead_field in ENUM_FIELDS
                    )
                else:
                    campos_vazios.append(lead_field)

            # Log dos campos de enumeração que foram convertidos
            if campos_convertidos:
                logger.info(
                    "enums_convertidos_lead_deal",
                    lead_id=lead_id,
                    total_convertidos=len(campos_convertidos),
                    detalhes=campos_convertidos
                )

            logger.info(
                "resumo_copia_campos_lead_deal",
                lead_id=lead_id,
                total_mapeados=len(LEAD_TO_DEAL_FIELD_MAP),
                campos_copiados=len(campos_copiados),
                campos_vazios=len(campos_vazios),
                campos_vazios_lista=campos_vazios
            )

        # Adiciona dados extras do agendamento se fornecidos
        # IMPORTANTE: Esses dados são passados diretamente do workflow para garantir
        # que os campos do deal sejam preenchidos mesmo se o lead ainda não foi atualizado
        if dados_agendamento:
            # Código do agendamento Belle
            if codigo_agendamento:
                deal_fields[DEAL_FIELD_CODIGO_AGENDAMENTO] = str(codigo_agendamento)

            # Nome do procedimento (texto)
            if dados_agendamento.get("servico_nome"):
                deal_fields[DEAL_FIELD_PROCEDIMENTO_NOME] = dados_agendamento["servico_nome"]

            # Data do agendamento - converter de "dd/mm/yyyy HH:MM:SS" para ISO format
            if dados_agendamento.get("data_agendamento"):
                data_str = dados_agendamento["data_agendamento"]
                try:
                    # Tenta parsear formato "dd/mm/yyyy HH:MM:SS"
                    dt = datetime.strptime(data_str, "%d/%m/%Y %H:%M:%S")
                    # Converte para ISO format com timezone de Brasília que o Bitrix aceita
                    data_iso = dt.strftime("%Y-%m-%dT%H:%M:%S-04:00")
                    deal_fields[DEAL_FIELD_DATA_AGENDAMENTO] = data_iso
                    logger.info("data_agendamento_convertida", original=data_str, iso=data_iso)
                except ValueError:
                    # Se não conseguir parsear, tenta usar valor original
                    logger.warning("data_agendamento_formato_invalido", valor=data_str)
                    deal_fields[DEAL_FIELD_DATA_AGENDAMENTO] = data_str

            # Profissional (código Belle -> ID Bitrix)
            if dados_agendamento.get("profissional"):
                profissional_belle = dados_agendamento["profissional"]
                profissional_bitrix = converter_belle_para_bitrix_profissional(profissional_belle)
                if profissional_bitrix:
                    deal_fields[DEAL_FIELD_PROFISSIONAL] = profissional_bitrix
                else:
                    # Se não encontrar mapeamento, tenta usar o valor original
                    logger.warning("profissional_usando_valor_original", valor=profissional_belle)

            # Estabelecimento (código Belle -> ID Bitrix)
            if dados_agendamento.get("estabelecimento"):
                estabelecimento_belle = dados_agendamento["estabelecimento"]
                estabelecimento_bitrix = converter_belle_para_bitrix_estabelecimento(estabelecimento_belle)
                if estabelecimento_bitrix:
                    deal_fields[DEAL_FIELD_ESTABELECIMENTO] = estabelecimento_bitrix
                else:
                    logger.warning("estabelecimento_usando_valor_original", valor=estabelecimento_belle)

            # Procedimento (nome -> ID Bitrix)
            if dados_agendamento.get("procedimento"):
                procedimento_str = dados_agendamento["procedimento"]
                procedimento_bitrix = converter_procedimento_para_bitrix(procedimento_str)
                if procedimento_bitrix:
                    deal_fields[DEAL_FIELD_PROCEDIMENTO] = [procedimento_bitrix]  # Campo é múltiplo
                else:
                    logger.warning("procedimento_nao_mapeado", valor=procedimento_str)

            # Também tenta pelo servico_nome se procedimento não funcionou
            if DEAL_FIELD_PROCEDIMENTO not in deal_fields and dados_agendamento.get("servico_nome"):
                servico_nome = dados_agendamento["servico_nome"]
                procedimento_bitrix = converter_procedimento_para_bitrix(servico_nome)
                if procedimento_bitrix:
                    deal_fields[DEAL_FIELD_PROCEDIMENTO] = [procedimento_bitrix]

            # Tipo de Atendimento - prioridade: parâmetro direto do workflow > tipo_consulta do lead
            if dados_agendamento.get("tipo_atendimento_direto"):
                # Já vem convertido do workflow
                deal_fields[DEAL_FIELD_TIPO_ATENDIMENTO] = dados_agendamento["tipo_atendimento_direto"]
                logger.info("tipo_atendimento_do_workflow_aplicado", valor=dados_agendamento["tipo_atendimento_direto"])
            elif dados_agendamento.get("tipo_consulta"):
                # Converte enum do lead para deal se necessário
                tipo_consulta = dados_agendamento["tipo_consulta"]
                tipo_convertido = converter_enum_lead_para_deal(LEAD_FIELD_TIPO_ATENDIMENTO, str(tipo_consulta))
                deal_fields[DEAL_FIELD_TIPO_ATENDIMENTO] = tipo_convertido
                logger.info("tipo_atendimento_do_lead_convertido", original=tipo_consulta, convertido=tipo_convertido)

            # Equipamento
            if dados_agendamento.get("equipamento"):
                deal_fields[DEAL_FIELD_EQUIPAMENTO] = dados_agendamento["equipamento"]

            # Código do cliente Belle
            if dados_agendamento.get("codigo_cliente_belle"):
                deal_fields[DEAL_FIELD_CODIGO_CLIENTE_BELLE] = dados_agendamento["codigo_cliente_belle"]

            # Origem do lead (enum - precisa conversão)
            if dados_agendamento.get("origem"):
                origem = dados_agendamento["origem"]
                origem_convertida = converter_enum_lead_para_deal(LEAD_FIELD_ORIGEM, origem)
                deal_fields[DEAL_FIELD_ORIGEM] = origem_convertida

            # Campanha (enum - precisa conversão)
            if dados_agendamento.get("campanha"):
                campanha = dados_agendamento["campanha"]
                campanha_convertida = converter_enum_lead_para_deal(LEAD_FIELD_CAMPANHA, campanha)
                deal_fields[DEAL_FIELD_CAMPANHA] = campanha_convertida

            # Tipo de paciente (enum - precisa conversão)
            if dados_agendamento.get("tipo_paciente"):
                tipo_paciente = dados_agendamento["tipo_paciente"]
                tipo_paciente_convertido = converter_enum_lead_para_deal(LEAD_FIELD_TIPO_PACIENTE, tipo_paciente)
                deal_fields[DEAL_FIELD_TIPO_PACIENTE] = tipo_paciente_convertido

            # Agendador
            if dados_agendamento.get("agendador"):
                deal_fields[DEAL_FIELD_AGENDADOR] = dados_agendamento["agendador"]

            # Segmento (código Belle -> ID Bitrix)
            if dados_agendamento.get("segmento"):
                segmento_belle = dados_agendamento["segmento"]
                segmento_bitrix = converter_belle_para_bitrix_segmento(str(segmento_belle))
                if segmento_bitrix:
                    deal_fields[DEAL_FIELD_SEGMENTO] = segmento_bitrix
                else:
                    logger.warning("segmento_usando_valor_original", valor=segmento_belle)

            # Procedimento (ativo) do Lead - copiar diretamente
            # O campo iblock_element pode ter o mesmo ID ou precisar mapeamento
            if dados_agendamento.get("procedimento_lead"):
                procedimento_lead = dados_agendamento["procedimento_lead"]
                # Primeiro tenta usar o valor diretamente (mesmo iblock)
                deal_fields[DEAL_FIELD_PROCEDIMENTO] = procedimento_lead
                logger.info("procedimento_lead_copiado", valor=procedimento_lead)

            logger.info(
                "campos_extras_adicionados_ao_deal",
                lead_id=lead_id,
                campos_extras=list(dados_agendamento.keys())
            )

        # Se temos código do agendamento mas não veio no dados_agendamento
        elif codigo_agendamento:
            deal_fields[DEAL_FIELD_CODIGO_AGENDAMENTO] = str(codigo_agendamento)

        logger.info(
            "criando_deal_com_campos",
            lead_id=lead_id,
            campos_deal=list(deal_fields.keys())
        )

        result = bitrix_call("crm.deal.add", {"fields": deal_fields})

        if result and result.get("result"):
            deal_id = result["result"]

            logger.info(
                "negocio_criado_do_lead",
                lead_id=lead_id,
                deal_id=deal_id,
                contact_id=contact_id
            )

            # Fecha o lead como convertido
            try:
                bitrix_call(
                    "crm.lead.update",
                    {
                        "id": lead_id,
                        "fields": {
                            "STATUS_ID": "CONVERTED",
                        }
                    }
                )
                logger.info("lead_marcado_convertido", lead_id=lead_id)
            except Exception as e:
                logger.warning("erro_marcar_lead_convertido", lead_id=lead_id, error=str(e))

            return {
                "success": True,
                "deal_id": deal_id,
                "contact_id": contact_id,
            }
        else:
            logger.warning("deal_add_sem_resultado", lead_id=lead_id, result=result)
            return {"success": False, "deal_id": None, "contact_id": None}

    except Exception as e:
        logger.error("erro_criar_negocio", lead_id=lead_id, error=str(e))
        return {"success": False, "deal_id": None, "contact_id": None, "error": str(e)}


def validar_estabelecimento(estabelecimento_codigo: int, estabelecimento_nome: str | None) -> str | None:
    """
    Valida se o estabelecimento está correto.
    Retorna mensagem de aviso se houver problema.
    """
    # Lista de estabelecimentos válidos (códigos Belle)
    estabelecimentos_validos = {
        1: "CLINICA CREPALDI DERMATO",
        2: "SPA CREPALDI",
        5: "CLINICA DERMATO E CONVENIOS LTDA",
        10: "DRIPS CLINIC",
        11: "CREPALDI CLINICA DE ESTETICA LTDA",
        12: "ESPACO BELA LASER",
        14: "KLAYNE MOURA SERVICOS MEDICOS LTDA",
    }

    if estabelecimento_codigo not in estabelecimentos_validos:
        return f"Estabelecimento {estabelecimento_codigo} ({estabelecimento_nome}) pode estar incorreto!"

    return None


@app.get("/")
async def root():
    """Endpoint de health check."""
    return {"status": "ok", "service": "Agendamento Webhook"}


@app.post("/webhook/agendar-json", response_model=AgendamentoResponse)
async def processar_agendamento_json(request: Request, dados: AgendamentoRequest):
    """
    Processa agendamento recebido do Bitrix.

    1. Valida estabelecimento
    2. Envia para Belle Software
    3. Atualiza campos do lead
    4. Adiciona comentário na timeline
    5. Move para etapa "Agendados"
    """
    logger.info(
        "webhook_recebido",
        lead_id=dados.lead_id,
        estabelecimento=dados.estabelecimento_codigo,
        profissional=dados.profissional_codigo,
    )

    warning = None

    try:
        # 1. Valida estabelecimento
        aviso_estabelecimento = validar_estabelecimento(
            dados.estabelecimento_codigo,
            dados.estabelecimento_nome
        )
        if aviso_estabelecimento:
            warning = aviso_estabelecimento
            adicionar_comentario_lead(
                dados.lead_id,
                f"AVISO: {aviso_estabelecimento}"
            )

        # 2. Prepara payload para Belle Software (formato oficial da API)
        servicos_lista = [s.strip() for s in dados.servicos.split(",") if s.strip()]

        # Monta array de serviços no formato da API Belle
        # IMPORTANTE: O campo tempo é OBRIGATÓRIO dentro de cada serviço
        serv_array = []
        for servico in servicos_lista:
            serv_array.append({
                "codServico": servico,
                "nomeServico": servico,
                "tempo": dados.tempo,  # OBRIGATÓRIO - duração em minutos
            })

        # Payload no formato oficial da API Belle
        belle_payload = {
            "codCli": int(dados.codigo_cliente_belle) if dados.codigo_cliente_belle else None,
            "codEstab": dados.estabelecimento_codigo,
            "prof": {
                "cod_usuario": str(dados.profissional_codigo),
                "nom_usuario": dados.profissional_nome or "",
            },
            "dtAgd": dados.data_agendamento,
            "hri": dados.horario,
            "serv": serv_array,
            "codPlano": "",
            "agSala": False,
            "codSala": 0,
            "codVendedor": "",
            "codEquipamento": dados.equipamento_codigo,
            "obs": dados.observacao,  # Campo abreviado
            "observacao": dados.observacao,  # Campo completo
        }

        # 3. Envia para Belle Software
        logger.info("enviando_para_belle", lead_id=dados.lead_id)
        belle_response = belle_call("/api/release/controller/IntegracaoExterna/v1.0/agenda/gravar", belle_payload)

        codigo_agendamento = belle_response.get("codAgendamento") or belle_response.get("codigo_agendamento")

        if not codigo_agendamento:
            raise HTTPException(
                status_code=500,
                detail=f"Belle não retornou código de agendamento: {belle_response}"
            )

        logger.info(
            "agendamento_criado_belle",
            lead_id=dados.lead_id,
            codigo_agendamento=codigo_agendamento,
        )

        # 4. Atualiza campos do lead no Bitrix
        data_formatada = f"{dados.data_agendamento} {dados.horario}:00"

        # Converte para formato ISO com timezone de Brasília para o Bitrix aceitar
        try:
            dt = datetime.strptime(data_formatada, "%d/%m/%Y %H:%M:%S")
            data_iso = dt.strftime("%Y-%m-%dT%H:%M:%S-04:00")
        except ValueError:
            # Se falhar, usa formato original
            data_iso = data_formatada
            logger.warning("data_lead_formato_invalido", valor=data_formatada)

        campos_atualizar = {
            FIELD_DATA_AGENDAMENTO: data_iso,
            FIELD_CODIGO_AGENDAMENTO: str(codigo_agendamento),
            FIELD_PROFISSIONAL: dados.profissional_nome or str(dados.profissional_codigo),
            FIELD_ESTABELECIMENTO: dados.estabelecimento_nome or str(dados.estabelecimento_codigo),
            FIELD_PROCEDIMENTO: dados.servicos,
            FIELD_TIPO_CONSULTA: dados.tipo_agendamento,
        }

        if dados.equipamento_nome:
            campos_atualizar[FIELD_EQUIPAMENTO] = dados.equipamento_nome

        if dados.agendador:
            campos_atualizar[LEAD_FIELD_AGENDADOR] = dados.agendador

        atualizar_lead(dados.lead_id, campos_atualizar)
        logger.info("lead_atualizado", lead_id=dados.lead_id)

        # 5. Monta comentário de sucesso com nomes bonitos
        nome_estabelecimento = BELLE_ESTABELECIMENTO_NOMES.get(dados.estabelecimento_codigo, dados.estabelecimento_nome or f"Estabelecimento {dados.estabelecimento_codigo}")

        comentario = f"""✅ Agendamento Criado com Sucesso!

📅 Data: {dados.data_agendamento}
🕐 Horário: {dados.horario}
🏥 Estabelecimento: {nome_estabelecimento}
👨‍⚕️ Profissional: {dados.profissional_nome or dados.profissional_codigo}
💆 Serviço: {dados.servicos}

📋 Código Agendamento Belle: {codigo_agendamento}
"""
        if dados.equipamento_nome:
            comentario += f"🔧 Equipamento: {dados.equipamento_nome}\n"

        adicionar_comentario_lead(dados.lead_id, comentario)

        # 6. Converte lead em negócio na etapa "Agendados"
        # Passa os dados do agendamento para garantir que os campos do deal sejam preenchidos
        dados_extras = {
            "servico_nome": dados.servicos,
            "data_agendamento": data_formatada,
            "codigo_agendamento": str(codigo_agendamento),
            "profissional": str(dados.profissional_codigo),
            "estabelecimento": str(dados.estabelecimento_codigo),
            "procedimento": dados.servicos,
            "tipo_consulta": dados.tipo_agendamento,
            "equipamento": dados.equipamento_nome,
            "codigo_cliente_belle": dados.codigo_cliente_belle,
        }

        # Agendador (quem executou o workflow)
        if dados.agendador:
            dados_extras["agendador"] = dados.agendador

        # Busca campos do lead original (origem, campanha, etc.) para copiar para o deal
        try:
            lead_response = bitrix_call("crm.lead.get", {"id": dados.lead_id})
            if lead_response and lead_response.get("result"):
                lead_info = lead_response["result"]

                # Origem do lead
                origem = lead_info.get(LEAD_FIELD_ORIGEM)
                if origem:
                    dados_extras["origem"] = origem

                # Campanha
                campanha = lead_info.get(LEAD_FIELD_CAMPANHA)
                if campanha:
                    dados_extras["campanha"] = campanha

                # Tipo de paciente
                tipo_paciente = lead_info.get(LEAD_FIELD_TIPO_PACIENTE)
                if tipo_paciente:
                    dados_extras["tipo_paciente"] = tipo_paciente

                # Agendador
                agendador = lead_info.get(LEAD_FIELD_AGENDADOR)
                if agendador:
                    dados_extras["agendador"] = agendador

                # Segmento
                segmento = lead_info.get(LEAD_FIELD_SEGMENTO)
                if segmento:
                    dados_extras["segmento"] = segmento

                # Procedimento (ativo) do Lead - para copiar para o Deal
                procedimento_lead = lead_info.get(LEAD_FIELD_PROCEDIMENTO)
                if procedimento_lead:
                    dados_extras["procedimento_lead"] = procedimento_lead

                logger.info(
                    "campos_lead_para_deal_json",
                    lead_id=dados.lead_id,
                    origem=origem,
                    campanha=campanha,
                    tipo_paciente=tipo_paciente,
                    procedimento=procedimento_lead
                )
        except Exception as e:
            logger.warning("erro_buscar_campos_lead", lead_id=dados.lead_id, error=str(e))

        conversao = converter_lead_para_negocio(dados.lead_id, str(codigo_agendamento), dados_extras)

        if conversao.get("success") and conversao.get("deal_id"):
            logger.info("negocio_criado", lead_id=dados.lead_id, deal_id=conversao.get("deal_id"))
        else:
            logger.warning("falha_criar_negocio", lead_id=dados.lead_id, resultado=conversao)

        return AgendamentoResponse(
            success=True,
            message="Agendamento processado com sucesso",
            codigo_agendamento=str(codigo_agendamento),
            lead_id=dados.lead_id,
            warning=warning,
        )

    except httpx.HTTPError as e:
        logger.error(
            "erro_http_agendamento",
            lead_id=dados.lead_id,
            error=str(e),
        )

        # Adiciona comentário de erro
        adicionar_comentario_lead(
            dados.lead_id,
            f"Erro ao criar agendamento:\n{str(e)}"
        )

        raise HTTPException(status_code=500, detail=str(e))

    except Exception as e:
        logger.error(
            "erro_processar_agendamento",
            lead_id=dados.lead_id,
            error=str(e),
        )

        # Adiciona comentário de erro
        adicionar_comentario_lead(
            dados.lead_id,
            f"Erro ao processar agendamento:\n{str(e)}"
        )

        raise HTTPException(status_code=500, detail=str(e))


@app.api_route("/webhook/agendar", methods=["GET", "POST"])
async def processar_agendamento_get(
    request: Request,
    lead_id: int = Query(..., description="ID do lead"),
    lead_nome: str = Query(None, description="Nome do lead"),
    lead_telefone: str = Query(None, description="Telefone do lead"),
    codigo_cliente_belle: str = Query(None, alias="codigo_cliente_belle", description="Codigo do cliente no Belle"),
    dataagendamento: str = Query("", description="Data dd/mm/yyyy"),
    horario: str = Query("", description="Horario HH:MM"),
    profissional: str = Query("", description="Codigo do profissional"),
    profissional_nome: str = Query(None, description="Nome do profissional"),
    estabelecimento: str = Query("", description="Codigo do estabelecimento"),
    tipoagenda: str = Query("Consulta", description="Tipo de agendamento"),
    procedimento: str = Query("", description="Servicos separados por virgula"),
    equipamento: str = Query(None, description="Codigo do equipamento"),
    obs: str = Query("", description="Observacao para o agendamento Belle"),
    responsavel: str = Query(None, description="Responsavel"),
    tempo: int = Query(30, description="Duracao em minutos"),
    situacao: str = Query(None, description="Situacao do negocio"),
    tipo_atendimento: str = Query(None, description="Tipo de Atendimento no Deal (ex: Baixa de Sessão, Consulta, etc.)"),
):
    """
    Processa agendamento via query parameters (GET/POST).
    Usado pelo workflow do Bitrix.
    """
    logger.info(
        "webhook_get_recebido",
        lead_id=lead_id,
        dataagendamento=dataagendamento,
        horario=horario,
        estabelecimento=estabelecimento,
        profissional=profissional,
    )

    warning = None

    # Valida campos obrigatórios
    campos_faltando = []
    if not dataagendamento:
        campos_faltando.append("Data do Agendamento (dataagendamento)")
    if not horario:
        campos_faltando.append("Horário (horario)")
    if not profissional:
        campos_faltando.append("Profissional (profissional)")
    if not estabelecimento:
        campos_faltando.append("Estabelecimento (estabelecimento)")
    if not procedimento:
        campos_faltando.append("Procedimento/Serviço (procedimento) - OBRIGATÓRIO para API Belle")

    if campos_faltando:
        erro_msg = f"Campos obrigatórios não preenchidos: {', '.join(campos_faltando)}"
        logger.error("campos_faltando", lead_id=lead_id, campos=campos_faltando)

        # Adiciona comentário no lead informando o erro
        adicionar_comentario_lead(
            lead_id,
            f"❌ Erro ao agendar - Parâmetros faltando:\n\n{erro_msg}\n\nVerifique se os parâmetros do workflow estão configurados corretamente.\n\nO campo 'procedimento' deve conter o código do serviço Belle (ex: 4 para Consulta)."
        )

        return {
            "success": False,
            "message": erro_msg,
            "lead_id": lead_id,
            "campos_recebidos": {
                "dataagendamento": dataagendamento,
                "horario": horario,
                "profissional": profissional,
                "estabelecimento": estabelecimento,
                "tipoagenda": tipoagenda,
                "procedimento": procedimento,
            }
        }

    try:
        # 1. Converte o ID interno do Bitrix para código Belle
        estab_bitrix = int(estabelecimento) if estabelecimento else 0
        estab_belle = converter_estabelecimento_para_belle(estab_bitrix)

        logger.info(
            "conversao_estabelecimento",
            lead_id=lead_id,
            bitrix_id=estab_bitrix,
            belle_code=estab_belle
        )

        # 2. Valida estabelecimento
        aviso_estabelecimento = validar_estabelecimento(estab_belle, None)
        if aviso_estabelecimento:
            warning = aviso_estabelecimento
            adicionar_comentario_lead(lead_id, f"AVISO: {aviso_estabelecimento}")

        # 2.1 Valida se o profissional pertence ao estabelecimento
        prof_valido, nome_prof_validado, erro_prof = validar_profissional_no_estabelecimento(profissional, estab_belle)
        if not prof_valido:
            logger.error(
                "profissional_invalido_para_estabelecimento",
                lead_id=lead_id,
                profissional=profissional,
                estabelecimento=estab_belle,
                erro=erro_prof
            )
            adicionar_comentario_lead(
                lead_id,
                f"❌ Erro ao agendar - Profissional inválido:\n\n{erro_prof}\n\n"
                f"O profissional selecionado não pode atender neste estabelecimento."
            )
            return {
                "success": False,
                "message": erro_prof,
                "lead_id": lead_id,
                "erro_tipo": "profissional_invalido"
            }

        # 3. Usa código cliente Belle - SOMENTE do CONTATO vinculado ao Lead
        #    O campo "código do cliente no Belle" fica no CONTATO, não no Lead
        codigo_cliente_final = codigo_cliente_belle  # Do parâmetro da URL (se passado)
        lead_info = None

        logger.info(
            "iniciando_busca_codigo_cliente_belle",
            lead_id=lead_id,
            codigo_url=codigo_cliente_belle,
            campo_contato=FIELD_CODIGO_CLIENTE_BELLE_CONTATO
        )

        # Busca informações do lead para obter dados como nome e telefone
        try:
            lead_data = bitrix_call("crm.lead.get", {"id": lead_id})
            if lead_data and lead_data.get("result"):
                lead_info = lead_data["result"]
                logger.info(
                    "lead_info",
                    lead_id=lead_id,
                    lead_title=lead_info.get("TITLE"),
                    contact_id=lead_info.get("CONTACT_ID")
                )
        except Exception as e:
            logger.warning("erro_buscar_lead", lead_id=lead_id, error=str(e))

        # Busca código Belle SOMENTE no CONTATO vinculado ao lead
        if not codigo_cliente_final:
            logger.info("buscando_codigo_belle_no_contato", lead_id=lead_id)
            codigo_cliente_final = buscar_codigo_cliente_belle_no_contato(lead_id)
            if codigo_cliente_final:
                logger.info(
                    "cliente_belle_encontrado_no_contato",
                    lead_id=lead_id,
                    codigo_belle=codigo_cliente_final
                )

        if codigo_cliente_final:
            logger.info("cliente_belle_final", lead_id=lead_id, codigo_belle=codigo_cliente_final)
        else:
            logger.warning("cliente_belle_nao_encontrado_no_contato", lead_id=lead_id)

        # Usa a variável final daqui em diante
        codigo_cliente_belle = codigo_cliente_final

        # 3.1 Se não tem código de cliente Belle, cria um lead automaticamente
        cliente_criado_agora = False
        if not codigo_cliente_belle:
            # Pega nome e telefone do lead
            nome_cliente = lead_nome or "Cliente"
            telefone_cliente = lead_telefone or ""

            # Se conseguiu buscar info do lead no Bitrix, usa esses dados
            if lead_info:
                nome_cliente = lead_nome or lead_info.get("NAME") or lead_info.get("TITLE") or "Cliente"
                if not telefone_cliente:
                    phones = lead_info.get("PHONE", [])
                    if phones and isinstance(phones, list) and len(phones) > 0:
                        telefone_cliente = phones[0].get("VALUE", "")

            # Tenta pegar CPF do lead se disponível
            cpf_cliente = None
            if lead_info:
                # Campo de CPF no Bitrix (ajuste conforme o campo real)
                cpf_cliente = lead_info.get("UF_CRM_CPF") or lead_info.get("CPF")

            logger.info("criando_cliente_belle_automatico", lead_id=lead_id, nome=nome_cliente, telefone=telefone_cliente, tem_cpf=bool(cpf_cliente))

            try:
                cliente_response = criar_cliente_belle(
                    nome=nome_cliente,
                    telefone=telefone_cliente,
                    codEstab=estab_belle,
                    cpf=cpf_cliente,
                    lead_id=lead_id
                )

                # Extrai o código do cliente criado
                codigo_cliente_belle = (
                    cliente_response.get("codigo") or
                    cliente_response.get("codCliente") or
                    cliente_response.get("cod_cliente") or
                    cliente_response.get("id")
                )

                if codigo_cliente_belle:
                    logger.info("cliente_belle_criado", lead_id=lead_id, codigo_belle=codigo_cliente_belle)
                    cliente_criado_agora = True

                    # Atualiza o campo no Bitrix com o código criado
                    try:
                        atualizar_lead(lead_id, {FIELD_CODIGO_CLIENTE_BELLE: str(codigo_cliente_belle)})
                        logger.info("bitrix_atualizado_com_codigo_belle", lead_id=lead_id, codigo_belle=codigo_cliente_belle)
                    except Exception as e:
                        logger.warning("erro_atualizar_bitrix_codigo_belle", lead_id=lead_id, error=str(e))

            except Exception as e:
                logger.error("erro_criar_cliente_belle", lead_id=lead_id, error=str(e))
                # Retorna erro se não conseguiu criar o lead
                adicionar_comentario_lead(
                    lead_id,
                    f"❌ Erro ao criar cliente no Belle:\n\n{str(e)}\n\n"
                    f"Verifique se os dados do lead estão corretos."
                )
                return {
                    "success": False,
                    "message": f"Erro ao criar cliente no Belle: {str(e)}",
                    "lead_id": lead_id,
                }

        # Valida que temos um código de cliente
        if not codigo_cliente_belle:
            erro_msg = "Não foi possível obter ou criar código de cliente Belle."
            logger.error("cliente_belle_obrigatorio", lead_id=lead_id)
            return {
                "success": False,
                "message": erro_msg,
                "lead_id": lead_id,
            }

        # 4. Cria o agendamento na Belle
        # Parseia o procedimento do formato Bitrix: servico[CODE][nome]=NAME
        query_params = dict(request.query_params)
        servico_info = parse_bitrix_servico(procedimento, query_params)

        # Usa o tempo do serviço parseado, ou o tempo passado como parâmetro, ou default 30
        tempo_servico = servico_info.get("tempo") or tempo or 30

        logger.info(
            "criando_agendamento",
            lead_id=lead_id,
            codigo_cliente=codigo_cliente_belle,
            servico_code=servico_info.get("codServico"),
            servico_nome=servico_info.get("nomeServico"),
            tempo=tempo_servico
        )

        try:
            agendamento_response = criar_agendamento_belle(
                codCliente=int(codigo_cliente_belle) if codigo_cliente_belle else None,
                codServico=servico_info.get("codServico"),
                codEstab=estab_belle,
                data=dataagendamento,
                hora=horario,
                codProfissional=profissional,
                observacao=obs,
                tempo=tempo_servico
            )

            codigo_agendamento = (
                agendamento_response.get("codAgendamento") or
                agendamento_response.get("codigo_agendamento") or
                agendamento_response.get("codigo") or
                agendamento_response.get("id") or
                "CRIADO"
            )

        except Exception as e:
            logger.error("erro_criar_agendamento_belle", lead_id=lead_id, error=str(e))
            codigo_agendamento = f"ERRO: {str(e)}"
            raise

        logger.info(
            "agendamento_criado_belle",
            lead_id=lead_id,
            codigo_agendamento=codigo_agendamento,
        )

        # 5. Atualiza campos do lead no Bitrix
        data_formatada = f"{dataagendamento} {horario}:00"

        # Converte para formato ISO com timezone de Brasília para o Bitrix aceitar
        try:
            dt = datetime.strptime(data_formatada, "%d/%m/%Y %H:%M:%S")
            data_iso = dt.strftime("%Y-%m-%dT%H:%M:%S-04:00")
        except ValueError:
            # Se falhar, usa formato original
            data_iso = data_formatada
            logger.warning("data_lead_formato_invalido_legacy", valor=data_formatada)

        campos_atualizar = {
            FIELD_DATA_AGENDAMENTO: data_iso,
            FIELD_CODIGO_AGENDAMENTO: str(codigo_agendamento),
            FIELD_PROFISSIONAL: profissional,
            FIELD_ESTABELECIMENTO: estabelecimento,
            FIELD_PROCEDIMENTO: procedimento,
            FIELD_TIPO_CONSULTA: tipoagenda,
        }

        if equipamento:
            campos_atualizar[FIELD_EQUIPAMENTO] = equipamento

        if codigo_cliente_belle:
            campos_atualizar[FIELD_CODIGO_CLIENTE_BELLE] = str(codigo_cliente_belle)

        atualizar_lead(lead_id, campos_atualizar)
        logger.info("lead_atualizado", lead_id=lead_id)

        # 6. Monta comentário de sucesso com nomes bonitos
        nome_estabelecimento = BELLE_ESTABELECIMENTO_NOMES.get(estab_belle, f"Estabelecimento {estab_belle}")
        nome_servico = servico_info.get("nomeServico") or procedimento or "Serviço"
        # Usa o nome do profissional validado pela API Belle, ou do parâmetro, ou busca no mapeamento
        nome_profissional_final = nome_prof_validado or profissional_nome or buscar_nome_profissional(profissional)

        comentario = f"""✅ Agendamento Criado com Sucesso!

📅 Data: {dataagendamento}
🕐 Horário: {horario}
🏥 Estabelecimento: {nome_estabelecimento}
👨‍⚕️ Profissional: {nome_profissional_final}
💆 Serviço: {nome_servico}

📋 Código Agendamento Belle: {codigo_agendamento}
👤 Código Cliente Belle: {codigo_cliente_belle or 'N/A'}
"""
        if equipamento:
            comentario += f"🔧 Equipamento: {equipamento}\n"

        if obs:
            comentario += f"📝 Observação: {obs}\n"

        if cliente_criado_agora:
            comentario += "\n✨ Cliente criado automaticamente no Belle Software"

        adicionar_comentario_lead(lead_id, comentario)

        # 7. Converte lead em negócio na etapa "Agendados"
        # Passa todos os campos do agendamento para garantir que sejam copiados para o deal
        dados_extras = {
            "situacao": situacao,
            "profissional_nome": nome_profissional_final,
            "servico_nome": nome_servico,
            # Campos do agendamento para copiar diretamente para o deal
            "data_agendamento": data_formatada,
            "codigo_agendamento": str(codigo_agendamento),
            "profissional": profissional,
            "estabelecimento": estabelecimento,
            "procedimento": procedimento,
            "tipo_consulta": tipoagenda,
            "equipamento": equipamento,
            "codigo_cliente_belle": str(codigo_cliente_belle) if codigo_cliente_belle else None,
        }

        # Busca campos do lead original (origem, campanha, etc.) para copiar para o deal
        if lead_info:
            # Origem do lead
            origem = lead_info.get(LEAD_FIELD_ORIGEM)
            if origem:
                dados_extras["origem"] = origem

            # Campanha
            campanha = lead_info.get(LEAD_FIELD_CAMPANHA)
            if campanha:
                dados_extras["campanha"] = campanha

            # Tipo de paciente
            tipo_paciente = lead_info.get(LEAD_FIELD_TIPO_PACIENTE)
            if tipo_paciente:
                dados_extras["tipo_paciente"] = tipo_paciente

            # Agendador
            agendador = lead_info.get(LEAD_FIELD_AGENDADOR)
            if agendador:
                dados_extras["agendador"] = agendador

            # Segmento
            segmento = lead_info.get(LEAD_FIELD_SEGMENTO)
            if segmento:
                dados_extras["segmento"] = segmento

            # Procedimento (ativo) do Lead - para copiar para o Deal
            procedimento_lead = lead_info.get(LEAD_FIELD_PROCEDIMENTO)
            if procedimento_lead:
                dados_extras["procedimento_lead"] = procedimento_lead

            logger.info(
                "campos_lead_para_deal",
                lead_id=lead_id,
                origem=origem,
                campanha=campanha,
                tipo_paciente=tipo_paciente,
                agendador=agendador,
                segmento=segmento,
                procedimento=procedimento_lead
            )

        # Tipo de Atendimento - pode vir do parâmetro do workflow ou do lead
        if tipo_atendimento:
            # Converte texto para ID se necessário
            tipo_atend_upper = tipo_atendimento.strip().upper()
            tipo_atend_id = TIPO_ATENDIMENTO_TEXTO_PARA_ID.get(tipo_atend_upper, tipo_atendimento)
            dados_extras["tipo_atendimento_direto"] = tipo_atend_id
            logger.info("tipo_atendimento_do_workflow", valor_original=tipo_atendimento, id_convertido=tipo_atend_id)

        conversao = converter_lead_para_negocio(lead_id, str(codigo_agendamento), dados_extras)

        if conversao.get("success") and conversao.get("deal_id"):
            deal_id = conversao.get("deal_id")
            logger.info("negocio_criado", lead_id=lead_id, deal_id=deal_id)

            # Adiciona produto (procedimento) à aba Orçamento do Deal
            if nome_servico:
                adicionar_produto_ao_deal(deal_id, nome_servico)
        else:
            logger.warning("falha_criar_negocio", lead_id=lead_id, resultado=conversao)

        return {
            "success": True,
            "message": "Agendamento processado com sucesso",
            "codigo_agendamento": str(codigo_agendamento),
            "codigo_cliente_belle": str(codigo_cliente_belle) if codigo_cliente_belle else None,
            "lead_id": lead_id,
            "warning": warning,
        }

    except httpx.HTTPError as e:
        logger.error("erro_http_agendamento", lead_id=lead_id, error=str(e))
        adicionar_comentario_lead(lead_id, f"Erro ao criar agendamento:\n{str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

    except Exception as e:
        logger.error("erro_processar_agendamento", lead_id=lead_id, error=str(e))
        adicionar_comentario_lead(lead_id, f"Erro ao processar agendamento:\n{str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.api_route("/agendamentos/add/", methods=["GET", "POST"])
async def agendamentos_add_legacy(
    request: Request,
    ID: str = Query(None, description="ID do lead/negócio"),
    dtAgd: str = Query("", description="Data dd/mm/yyyy"),
    codEstab: str = Query("", description="Código do estabelecimento Belle"),
    nomeProf: str = Query("", description="Nome do profissional"),
    codProf: str = Query("", description="Código do profissional"),
    id_prof: str = Query("", description="ID do profissional"),
    codCli: str = Query("", description="Código do cliente Belle"),
    hri: str = Query("", description="Horário HH:MM"),
    entidade: str = Query("lead", description="Tipo de entidade (lead/deal)"),
    vendedor: str = Query("", description="Email do vendedor"),
    tipo_agendamento: str = Query("", description="Tipo de agendamento"),
    equipamento: str = Query("", description="Código do equipamento"),
    contatoId: str = Query("", description="ID do contato"),
    contatoCPF: str = Query("", description="CPF do contato"),
    contatoName: str = Query("", description="Nome do contato"),
    tempo: str = Query("15", description="Tempo em minutos"),
    novo_card: str = Query("", description="Flag novo card"),
    pipe: str = Query("", description="Category/Pipeline ID"),
    responsavel: str = Query("", description="Responsável"),
    id_item_estab: str = Query("", description="ID item estabelecimento"),
):
    """
    Endpoint legado compatível com o workflow antigo.
    Recebe parâmetros no formato do servidor 187.60.56.72:25256
    """
    # Captura todos os parâmetros da query string para serviços
    query_params = dict(request.query_params)

    # Extrai serviços dos parâmetros
    # Formato Bitrix: servico[ID][nome]=X&servico[ID][tempo]=Y
    # Formato antigo: serv[0], serv[1], etc
    servicos = []
    servicos_dict = {}

    for key, value in query_params.items():
        # Formato novo: servico[ID][campo]=valor
        if key.startswith("servico[") or key.startswith("servico%5B"):
            import re
            # Extrai o ID do serviço: servico[12345][nome] -> 12345
            # Suporta formato normal e URL-encoded (%5B = [, %5D = ])
            match = re.search(r'servico(?:\[|%5B)(\d+)(?:\]|%5D)', key)
            if match:
                serv_id = match.group(1)
                if serv_id not in servicos_dict:
                    servicos_dict[serv_id] = {"id": serv_id}
                # Extrai o campo (nome ou tempo)
                if "[nome]" in key or "%5Bnome%5D" in key:
                    servicos_dict[serv_id]["nome"] = value
                elif "[tempo]" in key or "%5Btempo%5D" in key:
                    # Remove caracteres inválidos do tempo
                    tempo_clean = "".join(c for c in value if c.isdigit())
                    servicos_dict[serv_id]["tempo"] = tempo_clean or "15"
        # Formato antigo: serv[0], serv[1], etc
        elif key.startswith("serv[") or key.startswith("serv%5B"):
            servicos.append(value)

    # Converte dict de serviços para lista de IDs
    if servicos_dict:
        servicos = list(servicos_dict.keys())

    # Se não encontrou array, tenta parâmetro único
    if not servicos and query_params.get("serv"):
        servicos = [query_params.get("serv")]

    logger.info(
        "agendamentos_add_legacy",
        ID=ID,
        dtAgd=dtAgd,
        codEstab=codEstab,
        codProf=codProf,
        codCli=codCli,
        hri=hri,
        contatoCPF=contatoCPF,
        contatoName=contatoName,
        servicos=servicos,
    )

    # Validação básica
    if not dtAgd or not hri:
        return {
            "success": False,
            "error": "Data (dtAgd) e horário (hri) são obrigatórios",
        }

    try:
        # 1. Converte estabelecimento se necessário
        estab_belle = int(codEstab) if codEstab else None
        if not estab_belle and id_item_estab:
            estab_bitrix = int(id_item_estab)
            estab_belle = converter_estabelecimento_para_belle(estab_bitrix)

        if not estab_belle:
            return {"success": False, "error": "Código do estabelecimento não informado"}

        logger.info("estabelecimento_convertido", estab_belle=estab_belle)

        # 2. Verifica/cria cliente Belle
        codigo_cliente = codCli if codCli else None

        # Se não tem código do cliente mas tem CPF, tenta criar
        if not codigo_cliente and contatoCPF:
            cpf_limpo = "".join(c for c in contatoCPF if c.isdigit())
            if cpf_limpo:
                logger.info("criando_cliente_com_cpf", cpf=cpf_limpo, nome=contatoName)
                try:
                    cliente_response = criar_cliente_belle(
                        nome=contatoName or "Cliente",
                        telefone="",
                        codEstab=estab_belle,
                        cpf=cpf_limpo
                    )
                    codigo_cliente = (
                        cliente_response.get("codCliente") or
                        cliente_response.get("codigo") or
                        cliente_response.get("cod_cliente") or
                        cliente_response.get("id")
                    )
                    if codigo_cliente:
                        logger.info("cliente_criado_com_cpf", codigo=codigo_cliente)
                except Exception as e:
                    logger.error("erro_criar_cliente_cpf", error=str(e))

        # 3. Prepara profissional
        cod_profissional = codProf or id_prof or ""

        # 4. Cria o agendamento na Belle
        logger.info(
            "criando_agendamento_legacy",
            codigo_cliente=codigo_cliente,
            estab=estab_belle,
            prof=cod_profissional,
            data=dtAgd,
            hora=hri,
            servicos=servicos,
        )

        # Monta array de serviços
        # IMPORTANTE: O campo tempo é OBRIGATÓRIO dentro de cada serviço
        # Códigos Belle são tipicamente 4-6 dígitos. IDs maiores são provavelmente IDs Bitrix
        tempo_default = int(tempo) if tempo and tempo.isdigit() else 30
        serv_array = []
        for serv in servicos:
            if serv and len(str(serv)) <= 6:  # Ignora IDs muito longos (provavelmente Bitrix, não Belle)
                # Usa tempo específico do serviço se disponível
                serv_tempo = tempo_default
                if servicos_dict and serv in servicos_dict:
                    serv_tempo_str = servicos_dict[serv].get("tempo", "")
                    serv_tempo = int(serv_tempo_str) if serv_tempo_str.isdigit() else tempo_default
                serv_array.append({
                    "codServico": str(serv),
                    "nomeServico": servicos_dict.get(serv, {}).get("nome", str(serv)) if servicos_dict else str(serv),
                    "tempo": serv_tempo,  # OBRIGATÓRIO - duração em minutos
                })

        # Log se serviços foram ignorados
        if servicos and not serv_array:
            logger.warning("servicos_ignorados", servicos_originais=servicos, motivo="IDs parecem ser do Bitrix, não códigos Belle")

        # Payload no formato da API Belle
        belle_payload = {
            "codCli": int(codigo_cliente) if codigo_cliente else None,
            "codEstab": estab_belle,
            "prof": {
                "cod_usuario": str(cod_profissional) if cod_profissional else "",
                "nom_usuario": nomeProf or "",
            },
            "dtAgd": dtAgd,
            "hri": hri,
            "serv": serv_array,
            "codPlano": "",
            "agSala": False,
            "codSala": 0,
            "codVendedor": vendedor or "",
            "codEquipamento": int(equipamento) if equipamento and equipamento.isdigit() else None,
            "obs": f"Entidade: {entidade}, Pipeline: {pipe}" if pipe else "",  # Campo abreviado
            "observacao": f"Entidade: {entidade}, Pipeline: {pipe}" if pipe else "",  # Campo completo
        }

        logger.info("payload_belle", payload=belle_payload)

        belle_response = belle_call("/api/release/controller/IntegracaoExterna/v1.0/agenda/gravar", belle_payload)

        codigo_agendamento = (
            belle_response.get("codAgendamento") or
            belle_response.get("codigo_agendamento") or
            belle_response.get("codigo") or
            belle_response.get("id") or
            "CRIADO"
        )

        logger.info("agendamento_criado_legacy", codigo=codigo_agendamento)

        # 5. Atualiza entidade no Bitrix (se for lead)
        if ID and entidade == "lead":
            try:
                campos_atualizar = {
                    FIELD_CODIGO_AGENDAMENTO: str(codigo_agendamento),
                    FIELD_DATA_AGENDAMENTO: f"{dtAgd} {hri}:00",
                }
                if codigo_cliente:
                    campos_atualizar[FIELD_CODIGO_CLIENTE_BELLE] = str(codigo_cliente)

                atualizar_lead(int(ID), campos_atualizar)
                adicionar_comentario_lead(
                    int(ID),
                    f"✅ Agendamento criado via workflow legado\n\nCódigo: {codigo_agendamento}\nCliente Belle: {codigo_cliente or 'N/A'}\nData: {dtAgd} {hri}"
                )
            except Exception as e:
                logger.warning("erro_atualizar_lead_legacy", error=str(e))

        return {
            "success": True,
            "message": "Agendamento criado com sucesso",
            "codigo_agendamento": str(codigo_agendamento),
            "codigo_cliente": str(codigo_cliente) if codigo_cliente else None,
            "belle_response": belle_response,
        }

    except httpx.HTTPError as e:
        logger.error("erro_http_legacy", error=str(e))
        error_detail = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_detail = f"{str(e)} - {e.response.text}"
        return {"success": False, "error": error_detail}

    except Exception as e:
        logger.error("erro_agendamento_legacy", error=str(e))
        return {"success": False, "error": str(e)}


@app.post("/webhook/bitrix")
async def webhook_bitrix_raw(request: Request):
    """
    Endpoint alternativo que recebe dados raw do Bitrix.
    Útil para debug e para workflows que enviam formato diferente.
    """
    try:
        # Tenta JSON primeiro
        try:
            body = await request.json()
        except:
            # Se não for JSON, tenta form data
            body = dict(await request.form())

        logger.info("webhook_bitrix_raw", body=body)

        # Extrai dados do formato do Bitrix
        lead_id = body.get("document_id[2]") or body.get("DOCUMENT_ID[2]") or body.get("lead_id")

        if not lead_id:
            return {"error": "lead_id não encontrado", "body_received": body}

        # Retorna os dados recebidos para debug
        return {
            "status": "received",
            "lead_id": lead_id,
            "data": body,
        }

    except Exception as e:
        logger.error("erro_webhook_raw", error=str(e))
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
