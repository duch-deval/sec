# Payment Calendar / Business Day Location Extraction

## Field Definition
The city or jurisdiction names that define what constitutes a "Business Day" for payment purposes.
These locations determine which local holidays exclude a day from being a valid payment date.

## Bloomberg Business Rules
- Extract city and jurisdiction names only — no CDR codes, no descriptions
- Multiple locations are valid and common — return all of them
- "New York" and "The City of New York" are the same — normalize to "New York"
- "State of New York" is a jurisdiction — return as "New York"
- "Province of Ontario" — return as "Ontario"
- "People's Republic of China" — return as "China"
- "TARGET" or "TARGET2" — return as "TARGET" (euro payment system, not a city)
- "U.S. Government Securities Business Day" — return as-is, recognized Bloomberg term
- "Legal Holiday" without a named location — return null, cannot resolve without cross-reference
- "Place of Payment" without a named location — return null, cannot resolve

## Output Format
Return JSON only, no preamble:
{"locations": ["<city1>", "<city2>"], "raw_match": "<verbatim substring>"}

## Examples

### Example 1 — Single city
Input: "Business Day means any day other than a Saturday or Sunday on which banking institutions in The City of New York are not authorized to close..."
Output: {"locations": ["New York"], "raw_match": "banking institutions in The City of New York are not authorized to close"}

### Example 2 — Multiple cities
Input: "Business Day means any day of the year, other than a Saturday, Sunday or any day on which major banks are closed for business in the Province of Ontario or the People's Republic of China or Hong Kong..."
Output: {"locations": ["Ontario", "China", "Hong Kong"], "raw_match": "Province of Ontario or the People's Republic of China or Hong Kong"}

### Example 3 — TARGET system
Input: "Business Day means any day that is not a Saturday or Sunday and that is neither a legal holiday nor a day on which commercial banks are authorized to close in The City of New York; provided such day is also a day on which the TARGET2 system is open..."
Output: {"locations": ["New York", "TARGET"], "raw_match": "The City of New York; provided such day is also a day on which the TARGET2 system is open"}

### Example 4 — U.S. Government Securities Business Day
Input: "Business Day means each Monday through Friday which is not a day on which banking institutions in New York are authorized to close, and also a U.S. Government Securities Business Day..."
Output: {"locations": ["New York", "U.S. Government Securities Business Day"], "raw_match": "banking institutions in New York are authorized to close, and also a U.S. Government Securities Business Day"}

### Example 5 — NEGATIVE: Legal Holiday without location
Input: "Business Day means any day that is not a Legal Holiday or a Saturday or Sunday..."
Output: null

## Edge Cases
- Fort Worth, Houston, Chicago — valid US cities, return as-is
- "banking institutions in New York or at a place of payment" — return "New York" only
- Delaware alongside New York — include both: ["New York", "Delaware"]
- England — return as "England"
