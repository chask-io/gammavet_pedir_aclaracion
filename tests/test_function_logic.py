import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from backend.function_logic import ACTOR_LAMBDA, FunctionBackend  # noqa: E402
from chask_foundation.backend.models import OrchestrationEvent  # noqa: E402


EVENT_ID = "11111111-2222-4333-8444-555555555555"
SESSION_ID = "66666666-2222-4333-8444-555555555555"


def _event(args=None):
    tool_args = {"mensaje": "Puedes aclarar que necesitas?", "reason": "ambiguous"} if args is None else args
    return OrchestrationEvent.model_validate(
        {
            "event_id": EVENT_ID,
            "event_type": "function_call",
            "branch": "test",
            "organization_customer_id": None,
            "customer": None,
            "connection_key": "test",
            "organization": {
                "organization_id": "99999999-aaaa-4bbb-8ccc-dddddddddddd",
                "organization_name": "Chask Dev",
            },
            "prompt": "",
            "pipeline_id": 27023,
            "orchestration_session_uuid": SESSION_ID,
            "internal_orchestration_session_uuid": None,
            "channel_id": None,
            "entry_point_channel": "whatsapp",
            "source": "agent",
            "target": "function",
            "plan": None,
            "extra_params": {
                "user_phone_number": "+56 9 1111 2222",
                "agent_phone_number": "1051240901403291",
                "tool_calls": [{"args": tool_args}],
            },
            "access_token": "access-token",
            "target_agent": None,
            "target_operator": None,
            "type": None,
            "status": None,
            "channels": None,
            "whatsapp_template_instance": None,
            "created_at": None,
        }
    )


class FakeOrchestrator:
    def __init__(self):
        self.calls = []

    def call(self, endpoint, **kwargs):
        self.calls.append({"endpoint": endpoint, **kwargs})
        if endpoint == "evolve_event":
            return {
                "status_code": 201,
                "uuid": "22222222-2222-4222-8222-222222222222",
                "extra_params": kwargs["extra_params"],
            }
        return {"status_code": 200}


def test_pedir_aclaracion_sends_exactly_one_whatsapp_and_marker(monkeypatch):
    orchestrator = FakeOrchestrator()
    monkeypatch.setattr("backend.function_logic.orchestrator_api_manager", orchestrator)

    result = FunctionBackend(_event()).process_request()

    assert "aclaracion enviada" in result
    whatsapp_calls = [
        call
        for call in orchestrator.calls
        if call["endpoint"] == "evolve_event"
        and call.get("event_type") == "response_to_whatsapp_message"
    ]
    assert len(whatsapp_calls) == 1
    assert whatsapp_calls[0]["prompt"] == "Puedes aclarar que necesitas?"

    dispatch_calls = [
        call
        for call in orchestrator.calls
        if call["endpoint"] == "evolve_event" and call.get("event_type") == "dispatch_event"
    ]
    assert len(dispatch_calls) == 1
    assert dispatch_calls[0]["extra_params"]["event_type"] == "conductor_clarification_sent"
    assert dispatch_calls[0]["extra_params"]["actor_lambda"] == ACTOR_LAMBDA


def test_pedir_aclaracion_requires_mensaje():
    with pytest.raises(ValueError, match="mensaje"):
        FunctionBackend(_event(args={})).process_request()
