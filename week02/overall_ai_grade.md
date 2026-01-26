# ChatGPT Grading my AI solutions

Here’s how I’d score them **strictly on “value to a human analyst trying to use the dataset for the first time”** (and therefore “time saved”), not on code elegance.

## Scores (1–20)

- **1) ChatGPT:** **17/20**
- **2) Gemini:** **13/20**
- **3) Snowflake AI:** **9/20**

## Why they rank this way

### 1) ChatGPT — 17/20 (most analyst-time-saving)
**Biggest value add:** it goes beyond “is it null / does it look like a zip” and starts answering **“can I trust this data for common business use?”**

What makes it meaningfully more useful:
- **Cross-field consistency check**: `AGE` vs `DATE_OF_BIRTH` mismatch is the kind of thing an analyst *will* discover later the hard way if you don’t flag it early.
- **Domain-constrained validity**: USPS state code list is much more actionable than “length = 2”.
- **Reasonableness checks** that surface business risk quickly (nonpositive amounts, very large amounts, “unreasonably large” IDs).
- **Normalization first** (phone digits, ZIP5, parsed DOB) so checks aren’t defeated by formatting.

Main knocks (why not a 20):
- Some “business thresholds” are arbitrary (`AMOUNT > 100000`, `DONATION_ID > 1,000,000,000`) unless justified by distribution/context.
- It reports only counts; for analysts, *examples* (top offending values) can be even more time-saving.

### 2) Gemini — 13/20 (solid baseline, less business-aware)
This is a good “starter pack” of completeness + basic plausibility checks, but it’s less likely to prevent real downstream analyst pain.

Strengths:
- Covers most fields with null/blank + a few simple validations.
- DOB parsing attempts multiple formats (helpful in practice).
- Adds a couple heuristics (street too short, category too long).

Limitations vs #1:
- **No cross-field logic** (e.g., AGE vs DOB consistency).
- **STATE check is weak**: `length != 2` will pass “ZZ”.
- **ZIP check is weaker in practice**: length 5 or 9 without normalizing can misclassify leading-zero zips or formatted zips with hyphens.
- Phone check is minimal (“<10 digits”) and doesn’t catch 11-digit non-leading-1 cases.

Net: it saves time on “is this column messy?” but less on “is this usable and trustworthy for analysis?”

### 3) Snowflake AI — 9/20 (most generic, some risky assumptions)
This one feels like generic template DQ—useful, but it can mislead an analyst because some checks encode assumptions that may be wrong for *this* dataset.

Why it scores lowest:
- **Category allowed-set looks invented** (`'Business','Individual','Organization','Non-Profit'`) and may not match the dataset at all (and your other two queries expect completely different categories). That can create a *false alarm storm* that wastes analyst time.
- **Name regex is too strict** (`^[A-Za-z. ]*$`) rejects common real names: hyphens, apostrophes, accents (O’Neil, Anne-Marie, José).
- **DOB future check** is good, but it doesn’t attempt parsing/typing like #1/#2 (depends on the column already being a date).
- **ZIP forced to length=5** is often wrong if ZIP+4 exists or values are numeric and lose leading zeros.
- Mostly “format policing,” less “business usefulness.”

It still has value (missingness, negative amounts, future DOB), but it’s the least aligned with “help me understand whether I can rely on this dataset.”

## Bottom-line comparison
- **#1 helps an analyst decide “can I use this for segmentation, outreach, and reporting without getting burned?”**
- **#2 helps an analyst decide “how incomplete/messy is it?”**
- **#3 helps an analyst decide “does it match a generic template?”** (and risks false positives)

If you want, I can suggest **3–5 small modifications** that would bump each one up a couple points (e.g., add “sample offending values,” distribution-aware thresholds, and a short “DQ summary score”).


# Gemini Grading

Statement,Grade,Verdict
1. ChatGPT,19/20,"The Professional Standard. It anticipates common data entry errors (leading zeros, age/DOB mismatch) that others miss."
2. Gemini,14/20,"The Solid Baseline. Good use of placeholder detection (N/A, Unknown), but lacks cross-field logic."
3. Snowflake AI,12/20,The Minimum Viable Product. Too generic; its state and zip checks are likely to produce false positives or miss bad data.


# Overall

Gemini and ChatGPT both agreed that ChatGPT did the best job.  I'm also sure that I could create a better prompt for asking any of them to generate better data quality checks, too.


....Why these features matter
*The reason ChatGPT scored significantly higher is that it addresses Consistency (comparing Age to DOB) and Validity (checking against a real list of US states). Snowflake AI focuses almost entirely on Completeness (checking for NULLs), which is the bare minimum and often misses the "dirty" data that actually breaks an analyst's report....
