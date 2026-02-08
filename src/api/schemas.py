from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime


class OrderResponse(BaseModel):
    order_id: str
    customer_name: Optional[str] = None
    email: Optional[EmailStr] = None
    product: Optional[str] = None
    price: float
    quantity: int = Field(ge=1)
    city: Optional[str] = None
    status: str
    order_time: Optional[datetime] = None


class CitySalesResponse(BaseModel):
    city: str
    total_sales: float
