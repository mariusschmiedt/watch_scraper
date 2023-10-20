# watch_extractor

Watch Extractor scrapes the platform https://www.chrono24.de for watches of a desired brand to generate recommendations if a found watch is worth to buy.

The recommendation is provided with the following KPI's
- average / minimum / maximum price: the found watch is underrated or rated too high
- absolute / percentage market share: the found watch is exclusive or not

## main.py
The brands to be scraped are specified in the list "brands
```python
brands = ["rolex", "audemarspiguet", "patekphilippe"]
```
The found watches are further written to an sql database

## watch_eval.py
The watches which have been stored in the library are evaluated regarding different criteria
- average / minimum / maximum price of one
- - brand
  - model
  - reference number
- absolute / percentage market share of one
- -brand
  - model
  - reference number

