from __future__ import annotations
import re
from typing import List
from pydantic import BaseModel, Field, model_validator


class IssueSizeExtraction(BaseModel):
    """LLM structured output for issue size fallback."""
    amount: float = Field(gt=0, description="Numeric issue size, must be positive")
    currency: str = Field(
        min_length=3,
        max_length=3,
        description="ISO 4217 currency code e.g. USD, CAD, GBP",
    )
    raw_match: str = Field(
        description="Verbatim substring from the source snippet that contains this value"
    )

    @model_validator(mode="after")
    def currency_uppercase(self) -> "IssueSizeExtraction":
        self.currency = self.currency.upper()
        return self

    @model_validator(mode="after")
    def reject_placeholder(self) -> "IssueSizeExtraction":
        if re.search(r'\[\s*\]|\[_+\]|\[\s*__\s*\]', self.raw_match):
            raise ValueError("raw_match contains a placeholder — not a real issue size")
        if self.amount < 100_000:
            raise ValueError(f"Amount {self.amount} is too small to be an issue size")
        if not re.search(r"[$€£¥]\s*[\d,]+|\d{3,},\d{3}", self.raw_match):
            raise ValueError("raw_match does not contain a numeric amount")
        return self

    def verify_raw_match(self, snippet: str) -> bool:
        """Return True only if raw_match is a substring of the source snippet."""
        return self.raw_match in snippet


class MaturityDateExtraction(BaseModel):
    """LLM structured output for maturity date fallback."""
    date_str: str = Field(
        description="Full maturity date as written e.g. 'May 1, 2031' or 'January 15, 2028'"
    )
    raw_match: str = Field(
        description="Verbatim substring from the source snippet that contains this date"
    )

    @model_validator(mode="after")
    def validate_date_format(self) -> "MaturityDateExtraction":
        if not re.search(r"\b(20|19)\d{2}\b", self.date_str):
            raise ValueError("date_str does not contain a valid year")
        if re.search(r"\d+\.\d+%", self.date_str):
            raise ValueError("date_str looks like a coupon rate, not a maturity date")
        months = "January|February|March|April|May|June|July|August|September|October|November|December"
        if not re.search(months, self.raw_match, re.IGNORECASE) and \
                not re.search(r"matur|\bdue\b|due and payable|payable on", self.raw_match, re.IGNORECASE):
            raise ValueError("raw_match does not contain a recognizable date reference")
        if re.search(r"\bdue\b", self.raw_match, re.IGNORECASE) and \
                not re.search(months, self.raw_match, re.IGNORECASE) and \
                re.fullmatch(r"20\d{2}", self.date_str.strip()):
            raise ValueError("raw_match has due+year only — no month, not a precise maturity date")
        if re.search(r"\d+\.\d+%", self.raw_match) and \
                not re.search(months, self.raw_match, re.IGNORECASE) and \
                re.fullmatch(r"20\d{2}", self.date_str.strip()):
            raise ValueError("raw_match looks like a security description, not a maturity date")
        return self

    def verify_raw_match(self, snippet: str) -> bool:
        """Return True only if raw_match is a substring of the source snippet."""
        return self.raw_match in snippet


class BDReferenceExtraction(BaseModel):
    """LLM structured output for business day by-reference fallback."""
    locations: List[str] = Field(
        min_length=1,
        description="List of city or jurisdiction names e.g. ['New York', 'London']",
    )
    raw_match: str = Field(
        description="Verbatim substring from the source snippet that contains these locations"
    )

    @model_validator(mode="after")
    def validate_locations(self) -> "BDReferenceExtraction":
        placeholders = {"<unknown>", "unknown", "n/a", "none", "null", "<city>", "<location>"}
        for loc in self.locations:
            if loc.strip().lower().strip("<>") in placeholders:
                raise ValueError(f"Location is a placeholder, not a real city: {loc}")
            if len(loc) > 60:
                raise ValueError(f"Location too long to be a city name: {loc[:40]}")
            if re.search(r"means|other than|saturday|sunday|banking|holiday|legal", loc, re.IGNORECASE):
                raise ValueError(f"Location looks like a BD clause, not a city: {loc[:40]}")
        return self

    def verify_raw_match(self, snippet: str) -> bool:
        """Return True only if raw_match is a substring of source AND contains a location name."""
        if self.raw_match not in snippet:
            return False
        for loc in self.locations:
            if loc.lower() in self.raw_match.lower():
                return True
        return False