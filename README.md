# derby-name-scraper

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bdunnette/derby-name-scraper/blob/master/derby_name_scraper.ipynb)

Scraping [derby names](https://en.wikipedia.org/wiki/Roller_derby#Derby_names) from publicly-accessible lists

## Description

Extract specimen data from StarLIMS and create a Parquet file for further analysis.

## Installation

### Prerequisites

- Python - available from [python.org](https://www.python.org/downloads/) or [Windows Store](https://apps.microsoft.com/detail/9nrwmjp3717k)
- [pipenv](https://pipenv.pypa.io/en/latest/installation.html)

### Clone the repository

```powershell
git clone https://github.com/bdunnette/derby-name-scraper.git
cd derby-name-scraper
```

### Install dependencies

```powershell
pipenv install
```

## Usage

Download the [WFTDA](https://wftda.com/), [RDR](https://rollerderbyroster.com/), and [DRC](http://www.derbyrollcall.com/) rosters and save them to the data directory:

```powershell
pipenv run python -m luigi --module name_scraper ScrapeWFTDA --local-scheduler
pipenv run python -m luigi --module name_scraper ScrapeRDR --local-scheduler  
pipenv run python -m luigi --module name_scraper ScrapeDRC --local-scheduler  
```

Combine the rosters into a single file:

```powershell
pipenv run python -m luigi --module name_scraper CombineNames --local-scheduler
```

Generate a list of unique names and numbers:

```powershell
pipenv run python -m luigi --module name_scraper NameList --local-scheduler
pipenv run python -m luigi --module name_scraper NumberList --local-scheduler
```

To generate an ASCII-only list of names:

```powershell
pipenv run python -m luigi --module name_scraper NameList --ascii-only --local-scheduler
```

To generate a tab-separated list of names with numbers:

```powershell
pipenv run python -m luigi --module name_scraper NameNumberList --local-scheduler
```