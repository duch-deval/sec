from __future__ import annotations
from typing import List
from pydantic import BaseModel, Field, model_validator


class IssueSizeExtraction(BaseModel):
    """
    Structured output for LLM issue size fallback.
    amount must be positive. currency must be a 3-letter ISO code.
    raw_match must be a verbatim substring of the input snippet.
    """
    amount: float = Field(gt=0, description="Numeric issue size, must be positive")
    currency: str = Field(
        min_length=3,
        max_length=3,
        description="ISO 4217 currency code e.g. USD, CAD, GBP"
    )
    raw_match: str = Field(
        description="Verbatim substring from the source snippet that contains this value"
    )

    @model_validator(mode="after")
    def currency_uppercase(self) -> "IssueSizeExtraction":
        self.currency = self.currency.upper()
        return self

    def verify_raw_match(self, snippet: str) -> bool:
        """Return True only if raw_match is a substring of the source snippet."""
        return self.raw_match in snippet


class MaturityDateExtraction(BaseModel):
    """
    Structured output for LLM maturity date fallback.
    date_str is the full date as written in the document e.g. 'May 1, 2031'.
    raw_match must be a verbatim substring of the input snippet.
    """
    date_str: str = Field(
        description="Full maturity date as written e.g. 'May 1, 2031' or 'January 15, 2028'"
    )
    raw_match: str = Field(
        description="Verbatim substring from the source snippet that contains this date"
    )

    def verify_raw_match(self, snippet: str) -> bool:
        """Return True only if raw_match is a substring of the source snippet."""
        return self.raw_match in snippet


class BDReferenceExtraction(BaseModel):
    """
    Structured output for LLM business day by-reference fallback.
    Used when BD definition defers to 'Legal Holiday' or 'Place of Payment'.
    locations must be city/jurisdiction names only — no CDR codes.
    raw_match must be a verbatim substring of the input snippet.
    """
    locations: List[str] = Field(
        min_length=1,
        description="List of city or jurisdiction names e.g. ['New York', 'London']"
    )
    raw_match: str = Field(
        description="Verbatim substring from the source snippet that contains these locations"
    )

    def verify_raw_match(self, snippet: str) -> bool:
        """Return True only if raw_match is a substring of the source snippet."""
        return self.raw_match in snippet