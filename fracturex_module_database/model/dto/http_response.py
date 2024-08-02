from pydantic import BaseModel

class HTTP_Response(BaseModel):
    success: bool
    message: str
    data: list[dict] | dict
    