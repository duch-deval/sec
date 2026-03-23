# Maturity Date Extraction

## Field Definition
The date on which the principal of the bond becomes due and payable.
Extract the full calendar date as written in the document.

## Bloomberg Business Rules
- Return the full date as written: "May 1, 2031" not "2031"
- Prefer the date from the security description or cover page
- If only a year appears in the security description (e.g. "Notes Due 2031"),
  look for the full date in the body text near "mature", "maturity date", "due and payable"
- Do not infer or construct a date — only extract verbatim
- For floating rate notes, maturity date is still a fixed calendar date
- For multi-tranche: each tranche has its own maturity date

## Output Format
Return JSON only, no preamble:
{"date_str": "<full date as written>", "raw_match": "<verbatim substring>"}

## Examples

### Example 1 — Full date in body text
Input: "7.25% Notes Due 2031...The Notes will mature on March 1, 2031..."
Output: {"date_str": "March 1, 2031", "raw_match": "mature on March 1, 2031"}

### Example 2 — Structured note maturity
Input: "5.219% Secured Fiber Network Revenue Term Notes, Series 2026-1, Class A-2...shall be due and payable on February 20, 2031..."
Output: {"date_str": "February 20, 2031", "raw_match": "due and payable on February 20, 2031"}

### Example 3 — Date with ordinal
Input: "...the principal amount hereof shall be payable on the 15th day of January, 2028..."
Output: {"date_str": "15th day of January, 2028", "raw_match": "payable on the 15th day of January, 2028"}

### Example 4 — Floating rate, still has fixed maturity
Input: "Floating Rate Subordinated Notes Due 2036...will mature on April 1, 2036..."
Output: {"date_str": "April 1, 2036", "raw_match": "will mature on April 1, 2036"}

### Example 5 — NEGATIVE: year only, no full date found
Input: "5.875% Notes Due 2036...principal amount outstanding on maturity in 2036..."
Output: null

## Edge Cases
- "due 2029" in title but "February 15, 2029" in body — return "February 15, 2029"
- Dates written as "15 February 2029" (European format) — return as written
- Interest payment dates are NOT maturity dates — ignore them
