import json
import logging
from typing import Any

import anthropic

from app.config import settings

logger = logging.getLogger(__name__)

CLASSIFICATION_VALUES = ("confirmed", "updated", "unclear", "refused", "opt_out")


def build_system_prompt(
    entity_type: str, column_mappings: dict, tenant_name: str
) -> str:
    """Build system prompt in Portuguese for the AI conversation engine.

    Args:
        entity_type: Type of entity (e.g. "property", "imovel").
        column_mappings: Dict mapping original column names to friendly names.
        tenant_name: Name of the tenant/company using the system.

    Returns:
        System prompt string in Portuguese.
    """
    fields_description = ""
    if column_mappings:
        lines = []
        for original_name, friendly_name in column_mappings.items():
            lines.append(f"  - {friendly_name} (campo: {original_name})")
        fields_description = "\n".join(lines)
    else:
        fields_description = "  (sem mapeamento de campos configurado)"

    prompt = f"""Você é um assistente de atualização de dados para {tenant_name}.
Sua função é conversar com proprietários de {entity_type} via WhatsApp para confirmar ou atualizar as informações cadastradas.

## Campos do cadastro de {entity_type}:
{fields_description}

## Instruções importantes:
1. Seja sempre educado, objetivo e profissional.
2. Comunique-se em português brasileiro informal mas respeitoso.
3. Apresente as informações atuais e pergunte se estão corretas ou precisam de atualização.
4. Aceite respostas parciais — o proprietário pode confirmar alguns campos e atualizar outros.
5. Se o proprietário demonstrar claramente que não deseja continuar (ex: "não quero", "remove meu número", "para de me contatar"), classifique como opt_out.
6. Se o proprietário recusar de forma clara mas sem pedir remoção definitiva, classifique como refused.
7. Se a resposta for ambígua ou incompleta, classifique como unclear e faça uma pergunta de acompanhamento.
8. Limite suas perguntas de acompanhamento ao mínimo necessário.

## Formato de resposta obrigatório (JSON):
Você DEVE responder APENAS com um objeto JSON válido com a seguinte estrutura:
{{
  "classification": "<confirmed|updated|unclear|refused|opt_out>",
  "classification_score": <0.0 a 1.0, confiança na classificação>,
  "updated_fields": {{<campo>: <novo_valor>}} ou null,
  "follow_up_message": "<mensagem para enviar ao proprietário>" ou null,
  "ai_reasoning": {{
    "summary": "<breve explicação da sua decisão>",
    "detected_intent": "<intenção identificada na resposta do proprietário>",
    "confidence_factors": ["<fator1>", "<fator2>"]
  }}
}}

## Regras de classificação:
- "confirmed": proprietário confirmou que os dados estão corretos, sem alterações.
- "updated": proprietário forneceu pelo menos um dado novo ou corrigido.
- "unclear": resposta ambígua, incompleta ou fora de contexto — envie follow_up_message para esclarecer.
- "refused": proprietário claramente não quer participar mas sem pedir opt-out permanente.
- "opt_out": proprietário pediu explicitamente para não ser contatado novamente.

Não inclua nenhum texto fora do JSON na sua resposta.
"""
    return prompt


def build_user_context(original_data: dict, column_mappings: dict) -> str:
    """Format original record data for AI context.

    Args:
        original_data: The original record data dict.
        column_mappings: Dict mapping original column names to friendly names.

    Returns:
        Formatted string describing the record's current data.
    """
    lines = ["Dados atuais do cadastro:"]
    for key, value in original_data.items():
        friendly_name = column_mappings.get(key, key)
        display_value = value if value is not None else "(não informado)"
        lines.append(f"  - {friendly_name}: {display_value}")
    return "\n".join(lines)


def process_response(
    conversation_history: list[dict],
    original_data: dict,
    owner_response: str,
    system_prompt: str,
) -> dict:
    """Call Claude API to classify and process the owner's response.

    Args:
        conversation_history: List of prior messages in {"role": str, "content": str} format.
        original_data: The original record data for context.
        owner_response: The latest message from the property owner.
        system_prompt: Pre-built system prompt from build_system_prompt().

    Returns:
        Dict with keys: classification, updated_fields, follow_up_message, ai_reasoning, classification_score.
    """
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    msgs: list[dict[str, Any]] = list(conversation_history)
    msgs.append({"role": "user", "content": owner_response})

    try:
        response = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=1024,
            system=system_prompt,
            messages=msgs,  # type: ignore[arg-type]
        )

        content_block = response.content[0]
        raw_content = content_block.text.strip()  # type: ignore[union-attr]

        # Strip markdown code fences if present
        if raw_content.startswith("```"):
            lines = raw_content.splitlines()
            # Remove first line (```json or ```) and last line (```)
            inner_lines = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
            raw_content = "\n".join(inner_lines).strip()

        parsed = json.loads(raw_content)

        classification = parsed.get("classification", "unclear")
        if classification not in CLASSIFICATION_VALUES:
            classification = "unclear"

        score = parsed.get("classification_score", 0.5)
        try:
            score = float(score)
            score = max(0.0, min(1.0, score))
        except (TypeError, ValueError):
            score = 0.5

        return {
            "classification": classification,
            "updated_fields": parsed.get("updated_fields"),
            "follow_up_message": parsed.get("follow_up_message"),
            "ai_reasoning": parsed.get("ai_reasoning", {}),
            "classification_score": score,
        }

    except json.JSONDecodeError as exc:
        logger.warning("Failed to parse AI response as JSON: %s", exc)
        return {
            "classification": "unclear",
            "updated_fields": None,
            "follow_up_message": "Desculpe, não consegui entender sua resposta. Poderia repetir de outra forma?",
            "ai_reasoning": {
                "summary": "JSON parse error in AI response",
                "raw_error": str(exc),
            },
            "classification_score": 0.0,
        }
    except anthropic.APIError as exc:
        logger.error("Anthropic API error: %s", exc)
        raise
