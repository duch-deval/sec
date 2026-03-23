# Issue Size Extraction

## Field Definition
The total principal amount of the bond offering, as stated in the indenture or note agreement.
This is the face value of the securities being issued, not a minimum, maximum, or incremental amount.

## Bloomberg Business Rules
- Extract the single definitive offering amount only
- All currencies supported: USD, CAD, AUD, BRL, MXN, CHF, CNY, HKD, NZD, SGD, EUR, GBP
- No minimum issue size — amounts under $1,000,000 are valid
- Prefer cover page amount over body text when both exist
- REJECT amounts preceded by: "at least", "not less than", "in excess of", "increments of",
  "up to", "maximum of", "minimum of"
- REJECT amounts in covenant basket or debt limitation contexts (near word "Indebtedness")
- REJECT bid solicitation context amounts
- If multiple amounts exist, prefer the largest on the cover page (title page states total)

## Output Format
Return JSON only, no preamble:
{"amount": <number>, "currency": "<ISO 4217 3-letter code>", "raw_match": "<verbatim substring>"}

## Examples

### Example 1 — Standard USD offering
Input: "...the Company hereby agrees to issue $500,000,000 aggregate principal amount of 5.00% Senior Notes due 2030..."
Output: {"amount": 500000000, "currency": "USD", "raw_match": "$500,000,000 aggregate principal amount of 5.00% Senior Notes due 2030"}

### Example 2 — Non-USD offering
Input: "...EUR 750,000,000 4.125% Notes due 2028 ISIN: XS1234567890..."
Output: {"amount": 750000000, "currency": "EUR", "raw_match": "EUR 750,000,000 4.125% Notes due 2028"}

### Example 3 — Small offering (no minimum)
Input: "...aggregate principal amount of $75,000,000 of its 7.25% Notes Due 2031..."
Output: {"amount": 75000000, "currency": "USD", "raw_match": "$75,000,000 of its 7.25% Notes Due 2031"}

### Example 4 — NEGATIVE: incremental amount, reject
Input: "...in integral multiples of $1,000 in excess of $2,000..."
Output: null

### Example 5 — NEGATIVE: covenant basket, reject
Input: "...shall not permit Indebtedness to exceed $500,000,000 at any time..."
Output: null

## Critical Rejection Rules

ALWAYS return null if:
- The amount field contains a placeholder like `$[ ]`, `$[__]`, `[ ]`, or any bracket placeholder
- The amount appears to be a minimum denomination (e.g. "$1,000", "$2,000") rather than a total offering size
- The text is a security description or note title, not a dollar amount
- No numeric dollar amount can be identified in the text

## Edge Cases
- "1,125,470,000" with no currency symbol — assume USD if document is SEC US filing
- Multi-tranche: if cover page shows total, use total; if per-tranche, extract per-tranche amount
- Amounts written as "one billion dollars" — do not extract, no numeric form present
