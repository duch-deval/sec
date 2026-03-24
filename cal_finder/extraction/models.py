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

    @model_validator(mode="after")
    def reject_placeholder(self) -> "IssueSizeExtraction":
        import re
        # Reject bracket placeholders like $[ ], $[__], [ ], etc.
        if re.search(r'\[\s*\]|\[_+\]|\[\s*__\s*\]', self.raw_match):
            raise ValueError("raw_match contains a placeholder — not a real issue size")
        # Reject if amount is suspiciously small (denomination, not offering size)
        if self.amount < 100_000:
            raise ValueError(f"Amount {self.amount} is too small to be an issue size")
        # raw_match must contain a currency symbol or large number
        if not re.search(r"[$€£¥]\s*[\d,]+|\d{3,},\d{3}", self.raw_match):
            raise ValueError("raw_match does not contain a numeric amount")
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

    @model_validator(mode="after")
    def validate_date_format(self) -> "MaturityDateExtraction":
        import re
        # date_str must contain a 4-digit year
        if not re.search(r"\b(20|19)\d{2}\b", self.date_str):
            raise ValueError("date_str does not contain a valid year")
        # date_str must not look like a security description (contain coupon rate like 4.400%)
        if re.search(r"\d+\.\d+%", self.date_str):
            raise ValueError("date_str looks like a coupon rate, not a maturity date")
        # raw_match must contain a month name or maturity-related keyword
        months = "January|February|March|April|May|June|July|August|September|October|November|December"
        if not re.search(months, self.raw_match, re.IGNORECASE) and not re.search(r"matur|\bdue\b|due and payable|payable on", self.raw_match, re.IGNORECASE):
            raise ValueError("raw_match does not contain a recognizable date reference")
        if re.search(r"\bdue\b", self.raw_match, re.IGNORECASE) and not re.search(months, self.raw_match, re.IGNORECASE) and re.fullmatch(r"20\d{2}", self.date_str.strip()):
            raise ValueError("raw_match has due+year only — no month, not a precise maturity date")
        # reject security descriptions masquerading as maturity dates (e.g. "4.400% Notes due 2031")
        # only applies when date_str is year-only — full dates with month names are fine
        if re.search(r"\d+\.\d+%", self.raw_match) and not re.search(months, self.raw_match, re.IGNORECASE) and re.fullmatch(r"20\d{2}", self.date_str.strip()):
            raise ValueError("raw_match looks like a security description, not a maturity date")
        return self

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

    @model_validator(mode="after")
    def validate_locations(self) -> "BDReferenceExtraction":
        import re
        # Reject placeholder values
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
        # raw_match must actually contain at least one of the extracted location names
        for loc in self.locations:
            if loc.lower() in self.raw_match.lower():
                return True
        return False
