import hashlib
import hmac
import logging

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


def _get_headers() -> dict:
    return {
        "Authorization": f"Bearer {settings.whatsapp_access_token}",
        "Content-Type": "application/json",
    }


def _messages_url() -> str:
    return f"{settings.whatsapp_api_url}/{settings.whatsapp_phone_number_id}/messages"


async def send_template_message(phone_number: str, template_params: dict) -> dict:
    """Send a WhatsApp template message via Meta Business API.

    Args:
        phone_number: Recipient phone number in E.164 format (e.g. "5511999990000").
        template_params: Dict containing at minimum:
            - template_name (str): Name of the approved WhatsApp template.
            - language_code (str): BCP 47 language code, e.g. "pt_BR".
            - components (list, optional): Template component parameters.

    Returns:
        Parsed JSON response from the Meta API.

    Raises:
        httpx.HTTPStatusError: If the API returns a non-2xx response.
    """
    template_name = template_params.get("template_name", "data_refresh_request")
    language_code = template_params.get("language_code", "pt_BR")
    components = template_params.get("components", [])

    payload: dict = {
        "messaging_product": "whatsapp",
        "to": phone_number,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language_code},
        },
    }
    if components:
        payload["template"]["components"] = components

    async with httpx.AsyncClient() as client:
        response = await client.post(
            _messages_url(),
            headers=_get_headers(),
            json=payload,
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()


async def send_message(phone_number: str, text: str) -> dict:
    """Send a free-form text message via WhatsApp Business API.

    Args:
        phone_number: Recipient phone number in E.164 format.
        text: Message body text.

    Returns:
        Parsed JSON response from the Meta API.

    Raises:
        httpx.HTTPStatusError: If the API returns a non-2xx response.
    """
    payload = {
        "messaging_product": "whatsapp",
        "to": phone_number,
        "type": "text",
        "text": {"body": text},
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(
            _messages_url(),
            headers=_get_headers(),
            json=payload,
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()


def verify_webhook_signature(payload: bytes, signature: str) -> bool:
    """Verify the X-Hub-Signature-256 header from Meta webhook requests.

    Args:
        payload: Raw request body bytes.
        signature: Value of the X-Hub-Signature-256 header (including "sha256=" prefix).

    Returns:
        True if the signature is valid, False otherwise.
    """
    if not signature.startswith("sha256="):
        return False

    expected_sig = signature[len("sha256=") :]
    computed = hmac.new(
        settings.whatsapp_app_secret.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(computed, expected_sig)


def parse_webhook_payload(payload: dict) -> dict | None:
    """Extract message details from a Meta WhatsApp webhook payload.

    Handles the standard webhook structure for incoming messages.

    Args:
        payload: Parsed JSON body of the webhook request.

    Returns:
        Dict with keys {message_id, phone_number, content, timestamp} if a
        message event is found, or None if the payload is not a message event.
    """
    try:
        entry = payload.get("entry", [])
        if not entry:
            return None

        changes = entry[0].get("changes", [])
        if not changes:
            return None

        value = changes[0].get("value", {})
        messages = value.get("messages", [])
        if not messages:
            return None

        message = messages[0]
        msg_type = message.get("type")

        # Only handle text messages for now
        if msg_type != "text":
            logger.info("Ignoring non-text WhatsApp message type: %s", msg_type)
            return None

        return {
            "message_id": message.get("id"),
            "phone_number": message.get("from"),
            "content": message.get("text", {}).get("body", ""),
            "timestamp": message.get("timestamp"),
        }
    except (KeyError, IndexError, TypeError) as exc:
        logger.warning("Failed to parse WhatsApp webhook payload: %s", exc)
        return None
