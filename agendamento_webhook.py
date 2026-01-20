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

# URLs e Tokens
# A API da Belle pode usar diferentes bases dependendo do endpoint
BELLE_BASE_URL = "https://app.bellesoftware.com.br"
BELLE_TOKEN = "f236029cecd084712f7b3ce12c3e0c14"
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


def criar_cliente_belle(nome: str, telefone: str, codEstab: int, email: str = None, cpf: str = None) -> dict[str, Any]:
    """
    Cria ou atualiza um cliente na Belle Software.
    Retorna os dados do cliente incluindo o código.
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

    if email:
        payload["email"] = email

    if cpf:
        cpf_limpo = "".join(c for c in cpf if c.isdigit())
        if cpf_limpo:
            payload["cpf"] = cpf_limpo

    logger.info("criando_cliente_belle", payload=payload)
    # Endpoint documentado da API Belle para salvar cliente
    return belle_call("/api/release/controller/IntegracaoExterna/v1.0/clientes/salvar", payload)


def buscar_cliente_por_telefone(telefone: str, codEstab: int) -> dict[str, Any] | None:
    """
    Busca cliente na Belle pelo telefone.
    Retorna None se não encontrar.
    """
    try:
        telefone_limpo = "".join(c for c in telefone if c.isdigit())
        if not telefone_limpo:
            return None

        # Busca clientes com filtro (a API pode não suportar filtro por telefone direto)
        # Por enquanto, vamos criar o cliente e a Belle irá verificar duplicidade
        return None
    except Exception as e:
        logger.warning("erro_buscar_cliente", telefone=telefone, error=str(e))
        return None


def criar_agendamento_belle(
    codCliente: int,
    codServico: str,
    codEstab: int,
    data: str,
    hora: str,
    codProfissional: str = None,
    observacao: str = None
) -> dict[str, Any]:
    """
    Cria um agendamento de serviço na Belle Software.
    Usa o formato documentado na API da Belle.
    """
    # Monta array de serviços no formato da API Belle
    serv_array = []
    if codServico:
        serv_array.append({
            "codServico": str(codServico),
            "nomeServico": str(codServico),
        })

    # Payload no formato oficial da API Belle (Postman)
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
        "obs": observacao or "",
    }

    logger.info("criando_agendamento_belle", payload=payload)
    # Endpoint documentado da API Belle para gravar agenda
    return belle_call("/api/release/controller/IntegracaoExterna/v1.0/agenda/gravar", payload)


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

        # 2. Prepara payload para Belle Software (formato oficial da API)
        servicos_lista = [s.strip() for s in dados.servicos.split(",") if s.strip()]

        # Monta array de serviços no formato da API Belle
        serv_array = []
        for servico in servicos_lista:
            serv_array.append({
                "codServico": servico,
                "nomeServico": servico,
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
            "obs": dados.observacao,
        }

        # 3. Envia para Belle Software
        logger.info("enviando_para_belle", lead_id=dados.lead_id)
        belle_response = belle_call("/agenda/gravar", belle_payload)

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

        # 3. Busca código Belle existente no lead (se houver)
        codigo_cliente_belle = None

        try:
            lead_data = bitrix_call("crm.lead.get", {"id": lead_id})
            if lead_data and lead_data.get("result"):
                lead_info = lead_data["result"]
                codigo_cliente_belle = lead_info.get(FIELD_CODIGO_CLIENTE_BELLE)
                if codigo_cliente_belle:
                    logger.info("cliente_belle_existente", lead_id=lead_id, codigo_belle=codigo_cliente_belle)
        except Exception as e:
            logger.warning("erro_buscar_lead", lead_id=lead_id, error=str(e))

        # NOTA: Criação de cliente na Belle exige CPF, então vamos direto para agendamento
        # A Belle pode criar o cliente automaticamente durante o agendamento

        # 4. Cria o agendamento na Belle
        servicos_lista = [s.strip() for s in procedimento.split(",") if s.strip()]
        primeiro_servico = servicos_lista[0] if servicos_lista else None

        logger.info("criando_agendamento", lead_id=lead_id, codigo_cliente=codigo_cliente_belle, servico=primeiro_servico)

        try:
            agendamento_response = criar_agendamento_belle(
                codCliente=int(codigo_cliente_belle) if codigo_cliente_belle else None,
                codServico=primeiro_servico,
                codEstab=estab_belle,
                data=dataagendamento,
                hora=horario,
                codProfissional=profissional,
                observacao=obs
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

        if codigo_cliente_belle:
            campos_atualizar[FIELD_CODIGO_CLIENTE_BELLE] = str(codigo_cliente_belle)

        atualizar_lead(lead_id, campos_atualizar)
        logger.info("lead_atualizado", lead_id=lead_id)

        # 6. Adiciona comentario de sucesso na timeline
        comentario = f"""✅ Agendamento Criado com Sucesso

Codigo do Agendamento: {codigo_agendamento}
Codigo Cliente Belle: {codigo_cliente_belle or 'N/A'}
Data: {dataagendamento}
Hora: {horario}
Profissional: {profissional}
Estabelecimento: {estabelecimento} (Belle: {estab_belle})
Servicos: {procedimento}
"""
        if equipamento:
            comentario += f"Equipamento: {equipamento}\n"

        adicionar_comentario_lead(lead_id, comentario)

        # 7. Move para etapa "Agendados" (converte lead e cria negocio)
        mover_para_etapa_agendados(lead_id)

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
