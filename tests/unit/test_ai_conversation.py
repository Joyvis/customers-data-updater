"""
Unit tests for the AI conversation service and outreach message-limit handling.

T18: AC-AI-1  — AI classifies "sim, está tudo correto" → confirmed
T19: AC-AI-2  — AI extracts price change → updated_data reflects new price
T20: AC-AI-3  — Ambiguous response → classification "unclear", follow_up_message set
T21: AC-AI-4  — Message limit reached → conversation FAILED + record DEAD_LETTER
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Batch,
    BatchRecord,
    BatchRecordStatus,
    BatchStatus,
    Conversation,
    ConversationStatus,
    Tenant,
)
from app.services import ai_conversation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_anthropic_response(payload: dict) -> MagicMock:
    """Return a mock that mimics anthropic.Anthropic().messages.create() return value."""
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text=json.dumps(payload))]
    return mock_response


SYSTEM_PROMPT = "Test system prompt."
EMPTY_HISTORY: list[dict] = []
ORIGINAL_DATA = {
    "phone_number": "5511999990001",
    "owner_name": "João",
    "price": "400000",
}


# ---------------------------------------------------------------------------
# T18: confirmed classification
# AC-AI-1: When owner plainly confirms data, AI must return classification="confirmed"
# ---------------------------------------------------------------------------


def test_t18_process_response_confirmed():
    """T18 — AC-AI-1: Clear confirmation maps to 'confirmed' classification."""
    api_payload = {
        "classification": "confirmed",
        "classification_score": 0.95,
        "updated_fields": None,
        "follow_up_message": None,
        "ai_reasoning": {"summary": "Owner confirmed"},
    }
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _make_mock_anthropic_response(
        api_payload
    )

    with patch("anthropic.Anthropic", return_value=mock_client):
        result = ai_conversation.process_response(
            conversation_history=EMPTY_HISTORY,
            original_data=ORIGINAL_DATA,
            owner_response="sim, está tudo correto",
            system_prompt=SYSTEM_PROMPT,
        )

    assert result["classification"] == "confirmed"
    assert result["classification_score"] == pytest.approx(0.95)
    assert result["updated_fields"] is None
    assert result["follow_up_message"] is None
    assert result["ai_reasoning"]["summary"] == "Owner confirmed"


# ---------------------------------------------------------------------------
# T19: updated classification with price extraction
# AC-AI-2: When owner provides new price, AI must return classification="updated"
#           and updated_fields must contain the new value.
# ---------------------------------------------------------------------------


def test_t19_process_response_price_update():
    """T19 — AC-AI-2: Price change response maps to 'updated' with updated_fields."""
    api_payload = {
        "classification": "updated",
        "classification_score": 0.9,
        "updated_fields": {"price": "450000"},
        "follow_up_message": None,
        "ai_reasoning": {"summary": "Price updated"},
    }
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _make_mock_anthropic_response(
        api_payload
    )

    with patch("anthropic.Anthropic", return_value=mock_client):
        result = ai_conversation.process_response(
            conversation_history=EMPTY_HISTORY,
            original_data=ORIGINAL_DATA,
            owner_response="O preço mudou, agora é R$450.000",
            system_prompt=SYSTEM_PROMPT,
        )

    assert result["classification"] == "updated"
    assert result["updated_fields"] == {"price": "450000"}
    assert result["classification_score"] == pytest.approx(0.9)


# ---------------------------------------------------------------------------
# T20: unclear classification with follow_up_message
# AC-AI-3: Ambiguous response must produce classification="unclear" and a follow-up
# ---------------------------------------------------------------------------


def test_t20_process_response_unclear_sets_followup():
    """T20 — AC-AI-3: Ambiguous response produces 'unclear' + follow_up_message."""
    api_payload = {
        "classification": "unclear",
        "classification_score": 0.3,
        "updated_fields": None,
        "follow_up_message": "Poderia esclarecer?",
        "ai_reasoning": {"summary": "Ambiguous"},
    }
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _make_mock_anthropic_response(
        api_payload
    )

    with patch("anthropic.Anthropic", return_value=mock_client):
        result = ai_conversation.process_response(
            conversation_history=EMPTY_HISTORY,
            original_data=ORIGINAL_DATA,
            owner_response="talvez",
            system_prompt=SYSTEM_PROMPT,
        )

    assert result["classification"] == "unclear"
    assert result["follow_up_message"] == "Poderia esclarecer?"
    assert result["classification_score"] == pytest.approx(0.3)


def test_t20_process_response_invalid_json_falls_back_to_unclear():
    """T20 edge case — Malformed JSON from API degrades gracefully to 'unclear'."""
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="this is not json at all")]
    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_response

    with patch("anthropic.Anthropic", return_value=mock_client):
        result = ai_conversation.process_response(
            conversation_history=EMPTY_HISTORY,
            original_data=ORIGINAL_DATA,
            owner_response="???",
            system_prompt=SYSTEM_PROMPT,
        )

    assert result["classification"] == "unclear"
    assert result["classification_score"] == 0.0


def test_t20_process_response_unknown_classification_normalised_to_unclear():
    """T20 edge case — Unknown classification value is normalized to 'unclear'."""
    api_payload = {
        "classification": "completely_unknown_value",
        "classification_score": 0.5,
        "updated_fields": None,
        "follow_up_message": None,
        "ai_reasoning": {},
    }
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _make_mock_anthropic_response(
        api_payload
    )

    with patch("anthropic.Anthropic", return_value=mock_client):
        result = ai_conversation.process_response(
            conversation_history=EMPTY_HISTORY,
            original_data=ORIGINAL_DATA,
            owner_response="some response",
            system_prompt=SYSTEM_PROMPT,
        )

    assert result["classification"] == "unclear"


# ---------------------------------------------------------------------------
# T21: message limit reached → conversation FAILED + record DEAD_LETTER
# AC-AI-4: When message_count reaches max_messages and AI returns "unclear",
#           the conversation must be FAILED and the record must be DEAD_LETTER.
# ---------------------------------------------------------------------------


async def test_t21_message_limit_reached_dead_letters_conversation(
    db_session: AsyncSession,
    tenant: Tenant,
):
    """T21 — AC-AI-4: Hitting message limit transitions conversation to FAILED/DEAD_LETTER."""
    from app.services.outreach import process_inbound_message

    batch = Batch(
        tenant_id=tenant.id,
        file_name="limit.csv",
        file_key="test/limit.csv",
        file_size=512,
        status=BatchStatus.OUTREACH,
        max_messages_per_conversation=5,
    )
    db_session.add(batch)
    await db_session.flush()

    record = BatchRecord(
        batch_id=batch.id,
        tenant_id=tenant.id,
        row_number=1,
        phone_number="5511999990010",
        owner_name="Fernanda Lima",
        entity_type="property",
        original_data={"phone_number": "5511999990010", "owner_name": "Fernanda Lima"},
        status=BatchRecordStatus.OUTREACH,
    )
    db_session.add(record)
    await db_session.flush()

    # message_count=4, max_messages=5 — one more message will hit the limit
    conversation = Conversation(
        batch_record_id=record.id,
        tenant_id=tenant.id,
        phone_number="5511999990010",
        status=ConversationStatus.IN_PROGRESS,
        message_count=4,
        max_messages=5,
    )
    db_session.add(conversation)
    await db_session.commit()

    unclear_ai_result = {
        "classification": "unclear",
        "classification_score": 0.3,
        "updated_fields": None,
        "follow_up_message": "Poderia esclarecer?",
        "ai_reasoning": {"summary": "Ambiguous response"},
    }

    with (
        patch(
            "app.services.outreach.ai_conversation.process_response",
            return_value=unclear_ai_result,
        ),
        patch("app.services.outreach.whatsapp.send_message", return_value={}),
        patch("app.services.outreach.whatsapp.send_template_message", return_value={}),
    ):
        await process_inbound_message(
            db=db_session,
            conversation_id=conversation.id,
            message_content="não sei bem",
            raw_payload={"from": "5511999990010"},
        )

    await db_session.refresh(conversation)
    await db_session.refresh(record)

    assert conversation.status == ConversationStatus.FAILED
    assert record.status == BatchRecordStatus.DEAD_LETTER
