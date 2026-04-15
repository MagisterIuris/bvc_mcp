"""
Data models for BVC (Bourse de Casablanca) market data.

Uses Pydantic v2 for validation, type coercion, and automatic cleaning.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class Stock(BaseModel):
    """Represents a single stock/security listed on the Casablanca Stock Exchange."""

    symbol: str = Field(alias="Symbol")
    name: str = Field(alias="Libelle")
    price: Optional[float] = Field(default=None, alias="Cours")
    variation: Optional[float] = Field(default=None, alias="Variation")
    open: Optional[float] = Field(default=None, alias="Ouverture")
    high: Optional[float] = Field(default=None, alias="PlusHaut")
    low: Optional[float] = Field(default=None, alias="PlusBas")
    volume_mad: Optional[float] = Field(default=None, alias="Volumes")
    quantity_traded: Optional[int] = Field(default=None, alias="QteEchangee")
    best_bid: Optional[float] = Field(default=None, alias="MeilleurDemande")
    bid_quantity: Optional[int] = Field(default=None, alias="QteAchat")
    best_ask: Optional[float] = Field(default=None, alias="MeilleurOffre")
    ask_quantity: Optional[int] = Field(default=None, alias="QteVente")
    reference_price: Optional[float] = Field(default=None, alias="CoursDeReferance")
    last_trade_datetime: Optional[datetime] = Field(default=None, alias="DateDernierCours")
    market_state: Optional[str] = Field(default=None, alias="Etat")
    segment_code: Optional[str] = Field(default=None, alias="CodeSegment")
    security_type_id: Optional[int] = Field(default=None, alias="IdTypeValeur")

    model_config = {"populate_by_name": True}

    @field_validator("name", mode="before")
    @classmethod
    def strip_name(cls, v: str) -> str:
        """Strip leading/trailing whitespace from the company name."""
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator(
        "price", "variation", "open", "high", "low",
        "volume_mad", "best_bid", "best_ask", "reference_price",
        mode="before",
    )
    @classmethod
    def empty_string_to_none_float(cls, v) -> Optional[float]:
        """Convert empty strings or whitespace-only values to None for float fields."""
        if v == "" or (isinstance(v, str) and v.strip() == ""):
            return None
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    @field_validator("quantity_traded", "bid_quantity", "ask_quantity", mode="before")
    @classmethod
    def empty_string_to_none_int(cls, v) -> Optional[int]:
        """Convert empty strings or whitespace-only values to None for integer fields."""
        if v == "" or (isinstance(v, str) and v.strip() == ""):
            return None
        if v is None:
            return None
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return None

    @field_validator("last_trade_datetime", mode="before")
    @classmethod
    def parse_trade_datetime(cls, v) -> Optional[datetime]:
        """Parse the BVC datetime format 'DD/MM/YYYY HH:MM:SS' into a datetime object."""
        if not v or (isinstance(v, str) and v.strip() == ""):
            return None
        if isinstance(v, datetime):
            return v
        try:
            return datetime.strptime(v.strip(), "%d/%m/%Y %H:%M:%S")
        except (ValueError, AttributeError):
            return None

    @property
    def is_tradeable(self) -> bool:
        """Return True if the stock has a valid price (was traded today)."""
        return self.price is not None

    @property
    def variation_pct_display(self) -> str:
        """Return a formatted variation string like '+3.66%' or '-4.74%'."""
        if self.variation is None:
            return "N/A"
        sign = "+" if self.variation >= 0 else ""
        return f"{sign}{self.variation:.2f}%"

    def to_dict(self) -> dict:
        """Return a plain dictionary representation suitable for JSON serialization."""
        return {
            "symbol": self.symbol,
            "name": self.name,
            "price": self.price,
            "variation": self.variation,
            "variation_display": self.variation_pct_display,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "volume_mad": self.volume_mad,
            "quantity_traded": self.quantity_traded,
            "best_bid": self.best_bid,
            "bid_quantity": self.bid_quantity,
            "best_ask": self.best_ask,
            "ask_quantity": self.ask_quantity,
            "reference_price": self.reference_price,
            "last_trade_datetime": (
                self.last_trade_datetime.isoformat() if self.last_trade_datetime else None
            ),
            "market_state": self.market_state,
            "segment_code": self.segment_code,
            "is_tradeable": self.is_tradeable,
        }


class MarketSnapshot(BaseModel):
    """Represents a full market data snapshot returned by the BVC endpoint."""

    success: bool
    last_modified: int = Field(alias="lastModified")
    timestamp: str
    timestamp_french: str = Field(alias="timestampFrench")
    stocks: list[Stock]

    model_config = {"populate_by_name": True}

    @property
    def tradeable_stocks(self) -> list[Stock]:
        """Return only stocks that have a valid price (actively traded today)."""
        return [s for s in self.stocks if s.is_tradeable]

    @property
    def market_state(self) -> str:
        """Infer the market state from any stock that has an Etat field set."""
        for stock in self.stocks:
            if stock.market_state:
                return stock.market_state
        return "Unknown"
