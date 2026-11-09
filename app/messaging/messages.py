from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.application.commands import ProcessPayment
from app.domain.payment import MAX_ATTEMPTS


class WorkMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    event_id: UUID
    payment_id: UUID
    attempt_no: int = Field(ge=1, le=MAX_ATTEMPTS, strict=True)

    def to_command(self) -> ProcessPayment:
        return ProcessPayment(self.event_id, self.payment_id, self.attempt_no)
