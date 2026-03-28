from pydantic import BaseModel


class UsageResponse(BaseModel):
    event_type: str
    count: int
    period: str
