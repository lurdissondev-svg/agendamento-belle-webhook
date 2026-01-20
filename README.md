# Agendamento Belle - Webhook Server

Webhook server que processa agendamentos do Bitrix24 para a Belle Software.

## Funcionalidades

1. Recebe dados de agendamento do workflow Bitrix
2. Valida estabelecimento e adiciona aviso se incorreto
3. Envia agendamento para Belle Software
4. Atualiza campos do lead no Bitrix
5. Adiciona comentario na timeline do lead
6. Move lead para etapa "Agendado" (converte e cria negocio)

## Instalacao

```bash
cd Agendamento-belle
pip install -r requirements.txt
```

## Executar

```bash
uvicorn agendamento_webhook:app --host 0.0.0.0 --port 8000 --reload
```

## Endpoints

### Health Check
```
GET /
```

### Processar Agendamento
```
POST /webhook/agendar
Content-Type: application/json

{
    "lead_id": 123456,
    "lead_nome": "Nome do Cliente",
    "lead_telefone": "11999999999",
    "codigo_cliente_belle": "12345",
    "data_agendamento": "20/01/2026",
    "horario": "14:00",
    "estabelecimento_codigo": 1,
    "estabelecimento_nome": "CLINICA CREPALDI DERMATO",
    "profissional_codigo": 10,
    "profissional_nome": "Dr. Fulano",
    "tipo_agendamento": "Consulta",
    "servicos": "Botox, Preenchimento",
    "tempo": 30,
    "equipamento_codigo": null,
    "equipamento_nome": null,
    "novo_card": false,
    "observacao": ""
}
```

### Debug (recebe dados raw)
```
POST /webhook/bitrix
```

## Configuracao no Bitrix

Configure o workflow para enviar um webhook HTTP POST para:
```
http://SEU_SERVIDOR:8000/webhook/agendar
```

## Campos Bitrix Atualizados

- `UF_CRM_1725475287` - Data do Agendamento
- `UF_CRM_1725475314` - Codigo do Agendamento
- `UF_CRM_1725475343` - Profissional
- `UF_CRM_1725475371` - Estabelecimento
- `UF_CRM_1725475399` - Procedimento
- `UF_CRM_1732829755` - Tipo de Consulta
- `UF_CRM_1732829791` - Equipamento
