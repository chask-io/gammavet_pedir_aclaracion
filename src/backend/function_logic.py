"""
Business logic for PedirAclaracionFn.

Org-specific Gammavet lambda: send exactly one clarification WhatsApp and emit
the conductor_clarification_sent marker. It does not mutate tenant route state.
"""

import logging
import re
from typing import Any

from chask_foundation.backend.models import OrchestrationEvent

try:
    from api.orchestrator_requests import orchestrator_api_manager
except ModuleNotFoundError:
    from chask_foundation.api.orchestrator_requests import orchestrator_api_manager

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ACTOR_LAMBDA = "gammavet_pedir_aclaracion"
BOT_PHONE_ID = "1051240901403291"


def _normalizar_telefono(telefono: str) -> str:
    return "".join(c for c in str(telefono) if c.isdigit())


class FunctionBackend:
    def __init__(self, orchestration_event: OrchestrationEvent):
        self.orchestration_event = orchestration_event
        logger.info(
            "PedirAclaracionFn initialized for org=%s",
            orchestration_event.organization.organization_id,
        )

    def process_request(self) -> str:
        args = self._extract_tool_args()
        message = str(args.get("mensaje") or "").strip()
        if not message:
            raise ValueError("Falta el parametro requerido 'mensaje'")

        self._send_whatsapp(message)
        self._emit_dispatch_event(
            "conductor_clarification_sent",
            {
                "reason": str(args.get("reason") or "").strip() or None,
                "driver_phone": self._driver_phone(),
                "ticket_id": str(args.get("ticket_id") or self.orchestration_event.orchestration_session_uuid or ""),
                "event_id": str(self.orchestration_event.event_id),
            },
        )
        return "Solicitud de aclaracion enviada al conductor."

    def _driver_phone(self) -> str:
        return self._phones_for_response()[0] or self._event_phone()

    def _phones_for_response(self) -> tuple[str | None, str | None]:
        args = self._extract_tool_args()
        extra_params = self.orchestration_event.extra_params or {}
        user_phone = (
            args.get("driver_phone")
            or extra_params.get("user_phone_number")
            or self._event_phone()
        )
        agent_phone = extra_params.get("agent_phone_number") or BOT_PHONE_ID
        if not user_phone or not agent_phone:
            phones = self._session_phones()
            user_phone = user_phone or phones.get("user_phone_number")
            agent_phone = agent_phone or phones.get("agent_phone_number")
        return (_normalizar_telefono(user_phone) if user_phone else None, agent_phone)

    def _event_phone(self) -> str:
        customer = getattr(self.orchestration_event, "customer", None)
        if customer and getattr(customer, "phone", None):
            return str(customer.phone).strip()

        extra_params = self.orchestration_event.extra_params or {}
        value = str(
            self._first_value(extra_params, "driver_phone", "user_phone_number", "phone", "from")
            or ""
        ).strip()
        if value:
            return value

        prompt = str(getattr(self.orchestration_event, "prompt", "") or "")
        digits = "".join(re.findall(r"\d+", prompt))
        return digits if len(digits) >= 8 else ""

    def _session_phones(self) -> dict[str, str]:
        session_uuid = self.orchestration_event.orchestration_session_uuid
        if not session_uuid:
            return {}
        try:
            response = orchestrator_api_manager.call(
                "get_orchestration_events",
                orchestration_session_id=str(session_uuid),
                access_token=self.orchestration_event.access_token,
                organization_id=self.orchestration_event.organization.organization_id,
            )
        except Exception as exc:
            logger.error("Error reading session phones: %s", exc)
            return {}

        events = response if isinstance(response, list) else response.get("orchestration_events", [])
        for event in events if isinstance(events, list) else []:
            if event.get("event_type") != "new_ticket":
                continue
            extra_params = event.get("extra_params") or {}
            phones: dict[str, str] = {}
            if extra_params.get("user_phone_number"):
                phones["user_phone_number"] = extra_params["user_phone_number"]
            if extra_params.get("agent_phone_number"):
                phones["agent_phone_number"] = extra_params["agent_phone_number"]
            if phones:
                return phones
        return {}

    def _send_whatsapp(self, text: str) -> None:
        user_phone, agent_phone = self._phones_for_response()
        if not user_phone or not agent_phone:
            raise ValueError("No se encontro telefono del conductor para pedir aclaracion")
        evolve_response = orchestrator_api_manager.call(
            "evolve_event",
            parent_event_uuid=str(self.orchestration_event.event_id),
            event_type="response_to_whatsapp_message",
            source="agent",
            target="orchestrator",
            prompt=text,
            extra_params={"user_phone_number": user_phone, "agent_phone_number": agent_phone},
            access_token=self.orchestration_event.access_token,
            organization_id=self.orchestration_event.organization.organization_id,
        )
        if evolve_response.get("status_code") not in (200, 201):
            raise RuntimeError(f"Failed to evolve WhatsApp event: {evolve_response.get('error')}")
        if not evolve_response.get("uuid"):
            raise RuntimeError("WhatsApp evolve_event response missing uuid")

        whatsapp_event = self.orchestration_event.model_copy(deep=True)
        whatsapp_event.event_id = evolve_response["uuid"]
        whatsapp_event.event_type = "response_to_whatsapp_message"
        whatsapp_event.source = "agent"
        whatsapp_event.target = "orchestrator"
        whatsapp_event.prompt = text
        whatsapp_event.extra_params = evolve_response.get("extra_params", {})
        orchestrator_api_manager.call(
            "forward_oe_to_kafka",
            orchestration_event=whatsapp_event.model_dump(),
            topic="orchestrator",
            access_token=whatsapp_event.access_token,
            organization_id=whatsapp_event.organization.organization_id,
        )

    def _emit_dispatch_event(self, event_type: str, metadata: dict[str, Any]) -> None:
        orchestrator_api_manager.call(
            "evolve_event",
            parent_event_uuid=str(self.orchestration_event.event_id),
            event_type="dispatch_event",
            source="agent",
            target="orchestrator",
            prompt=event_type,
            extra_params={
                "event_type": event_type,
                "actor_lambda": ACTOR_LAMBDA,
                "metadata": metadata,
            },
            access_token=self.orchestration_event.access_token,
            organization_id=self.orchestration_event.organization.organization_id,
        )

    def _extract_tool_args(self) -> dict[str, Any]:
        extra_params = self.orchestration_event.extra_params or {}
        tool_calls = extra_params.get("tool_calls", [])
        if not tool_calls:
            return {}
        args = tool_calls[0].get("args", {}) or {}
        return args if isinstance(args, dict) else {}

    def _first_value(self, data: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            value = data.get(key)
            if value:
                return value
        return None
