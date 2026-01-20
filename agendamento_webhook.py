"""
Webhook Server para processar agendamentos do Bitrix.

Recebe dados do workflow do Bitrix, envia para Belle Software,
atualiza o lead e move para etapa "Agendados".

Uso:
    uvicorn agendamento_webhook:app --host 0.0.0.0 --port 8000 --reload
"""

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

# URLs
BELLE_WEBHOOK_URL = "http://187.60.56.72:25256"
BITRIX_WEBHOOK_URL = "https://crepaldi.bitrix24.com.br/rest/126490/7ckld4dli6jds9b2"

# Campos do Lead no Bitrix
FIELD_DATA_AGENDAMENTO = "UF_CRM_1725475287"
FIELD_CODIGO_AGENDAMENTO = "UF_CRM_1725475314"
FIELD_PROFISSIONAL = "UF_CRM_1725475343"
FIELD_ESTABELECIMENTO = "UF_CRM_1725475371"
FIELD_PROCEDIMENTO = "UF_CRM_1725475399"
FIELD_TIPO_CONSULTA = "UF_CRM_1732829755"
FIELD_EQUIPAMENTO = "UF_CRM_1732829791"
FIELD_CODIGO_CLIENTE_BELLE = "UF_CRM_1702053628"

# Etapa "Agendado" no funil de Leads (converte lead e cria negócio)
ETAPA_AGENDADOS = "CONVERTED"

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


def belle_call(endpoint: str, payload: dict) -> dict[str, Any]:
    """Faz chamada à API da Belle Software."""
    url = f"{BELLE_WEBHOOK_URL}{endpoint}"
    try:
        response = httpx.post(url, json=payload, timeout=30.0)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        logger.error("belle_api_error", endpoint=endpoint, error=str(e))
        raise


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


def mover_para_etapa_agendados(lead_id: int) -> bool:
    """Move o lead para a etapa 'Agendados' (converte e cria negócio)."""
    try:
        result = bitrix_call(
            "crm.lead.update",
            {
                "id": lead_id,
                "fields": {
                    "STATUS_ID": ETAPA_AGENDADOS,
                }
            }
        )
        logger.info("lead_movido_agendados", lead_id=lead_id)
        return result.get("result", False)
    except Exception as e:
        logger.error("erro_mover_etapa", lead_id=lead_id, error=str(e))
        return False


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

        # 2. Prepara payload para Belle Software
        servicos_lista = [s.strip() for s in dados.servicos.split(",") if s.strip()]

        belle_payload = {
            "codCliente": dados.codigo_cliente_belle,
            "nomeCliente": dados.lead_nome,
            "telefoneCliente": dados.lead_telefone,
            "dataAgendamento": dados.data_agendamento,
            "horaAgendamento": dados.horario,
            "codEstabelecimento": dados.estabelecimento_codigo,
            "codProfissional": dados.profissional_codigo,
            "tipoAgendamento": dados.tipo_agendamento,
            "servicos": servicos_lista,
            "tempo": dados.tempo,
            "codEquipamento": dados.equipamento_codigo,
            "novoCard": dados.novo_card,
            "observacao": dados.observacao,
            "leadId": dados.lead_id,
        }

        # 3. Envia para Belle Software
        logger.info("enviando_para_belle", lead_id=dados.lead_id)
        belle_response = belle_call("/agendar", belle_payload)

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

        campos_atualizar = {
            FIELD_DATA_AGENDAMENTO: data_formatada,
            FIELD_CODIGO_AGENDAMENTO: str(codigo_agendamento),
            FIELD_PROFISSIONAL: dados.profissional_nome or str(dados.profissional_codigo),
            FIELD_ESTABELECIMENTO: dados.estabelecimento_nome or str(dados.estabelecimento_codigo),
            FIELD_PROCEDIMENTO: dados.servicos,
            FIELD_TIPO_CONSULTA: dados.tipo_agendamento,
        }

        if dados.equipamento_nome:
            campos_atualizar[FIELD_EQUIPAMENTO] = dados.equipamento_nome

        atualizar_lead(dados.lead_id, campos_atualizar)
        logger.info("lead_atualizado", lead_id=dados.lead_id)

        # 5. Adiciona comentário de sucesso na timeline
        comentario = f"""Agendamento Criado com Sucesso

Codigo do Agendamento: {codigo_agendamento}
Data: {dados.data_agendamento}
Hora: {dados.horario}
Profissional: {dados.profissional_nome or dados.profissional_codigo}
Estabelecimento: {dados.estabelecimento_nome or dados.estabelecimento_codigo}
Servicos: {dados.servicos}
"""
        if dados.equipamento_nome:
            comentario += f"Equipamento: {dados.equipamento_nome}\n"

        adicionar_comentario_lead(dados.lead_id, comentario)

        # 6. Move para etapa "Agendados" (converte lead e cria negócio)
        mover_para_etapa_agendados(dados.lead_id)

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
    lead_id: int = Query(..., description="ID do lead"),
    lead_nome: str = Query(None, description="Nome do lead"),
    lead_telefone: str = Query(None, description="Telefone do lead"),
    dataagendamento: str = Query("", description="Data dd/mm/yyyy"),
    horario: str = Query("", description="Horario HH:MM"),
    profissional: str = Query("", description="Codigo do profissional"),
    estabelecimento: str = Query("", description="Codigo do estabelecimento"),
    tipoagenda: str = Query("Consulta", description="Tipo de agendamento"),
    procedimento: str = Query("", description="Servicos separados por virgula"),
    equipamento: str = Query(None, description="Codigo do equipamento"),
    obs: str = Query("", description="Observacao"),
    responsavel: str = Query(None, description="Responsavel"),
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
        campos_faltando.append("Data do Agendamento")
    if not horario:
        campos_faltando.append("Horário")
    if not profissional:
        campos_faltando.append("Profissional")
    if not estabelecimento:
        campos_faltando.append("Estabelecimento")

    if campos_faltando:
        erro_msg = f"Campos obrigatórios não preenchidos: {', '.join(campos_faltando)}"
        logger.error("campos_faltando", lead_id=lead_id, campos=campos_faltando)

        # Adiciona comentário no lead informando o erro
        adicionar_comentario_lead(
            lead_id,
            f"❌ Erro ao agendar - Parâmetros faltando:\n\n{erro_msg}\n\nVerifique se os parâmetros do workflow estão configurados corretamente."
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

        # 3. Prepara payload para Belle Software
        servicos_lista = [s.strip() for s in procedimento.split(",") if s.strip()]

        belle_payload = {
            "codCliente": None,
            "nomeCliente": lead_nome,
            "telefoneCliente": lead_telefone,
            "dataAgendamento": dataagendamento,
            "horaAgendamento": horario,
            "codEstabelecimento": estab_belle,  # Usa código Belle convertido
            "codProfissional": int(profissional) if profissional else 0,
            "tipoAgendamento": tipoagenda,
            "servicos": servicos_lista,
            "tempo": 15,
            "codEquipamento": int(equipamento) if equipamento else None,
            "novoCard": False,
            "observacao": obs or "",
            "leadId": lead_id,
        }

        # 3. Envia para Belle Software
        logger.info("enviando_para_belle", lead_id=lead_id, payload=belle_payload)
        belle_response = belle_call("/agendar", belle_payload)

        codigo_agendamento = belle_response.get("codAgendamento") or belle_response.get("codigo_agendamento") or "N/A"

        logger.info(
            "agendamento_criado_belle",
            lead_id=lead_id,
            codigo_agendamento=codigo_agendamento,
        )

        # 4. Atualiza campos do lead no Bitrix
        data_formatada = f"{dataagendamento} {horario}:00"

        campos_atualizar = {
            FIELD_DATA_AGENDAMENTO: data_formatada,
            FIELD_CODIGO_AGENDAMENTO: str(codigo_agendamento),
            FIELD_PROFISSIONAL: profissional,
            FIELD_ESTABELECIMENTO: estabelecimento,
            FIELD_PROCEDIMENTO: procedimento,
            FIELD_TIPO_CONSULTA: tipoagenda,
        }

        if equipamento:
            campos_atualizar[FIELD_EQUIPAMENTO] = equipamento

        atualizar_lead(lead_id, campos_atualizar)
        logger.info("lead_atualizado", lead_id=lead_id)

        # 5. Adiciona comentario de sucesso na timeline
        comentario = f"""Agendamento Criado com Sucesso

Codigo do Agendamento: {codigo_agendamento}
Data: {dataagendamento}
Hora: {horario}
Profissional: {profissional}
Estabelecimento: {estabelecimento}
Servicos: {procedimento}
"""
        if equipamento:
            comentario += f"Equipamento: {equipamento}\n"

        adicionar_comentario_lead(lead_id, comentario)

        # 6. Move para etapa "Agendados" (converte lead e cria negocio)
        mover_para_etapa_agendados(lead_id)

        return {
            "success": True,
            "message": "Agendamento processado com sucesso",
            "codigo_agendamento": str(codigo_agendamento),
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
