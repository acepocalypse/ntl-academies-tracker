# National Academy Tracker
**Authors:** Ace Setiawan & Ana Farmus  
**Date:** September 2025

## 📄 Overview
This program automates web-scrapes of the **National Academy of Engineering**, **Medicine**, and **Science** websites.  
It logs **additions**, **removals**, and **modifications** to each site in `.csv` files.  

- Runs **weekly** on a dedicated server or computer.  
- Sends an **email alert** to the Purdue EDA team with links to the CSV reports showing changes.

---

## 📂 Directory Overview

```text
NTL-Academies-Tracker/
├─ .vscode/
│ └─ settings.json
│
├─ monitor/
│ ├─ __init__.py
| ├─ diff_utils.py
│ ├─ notify.py
│ ├─ orchestrator.py
│ ├─ run_all.py
│ ├─ send_email.py
│ └─ test_runner.py
│
├─ scrapers/
│ ├─ __init__.py
│ ├─ nae.py
│ ├─ nam.py
│ ├─ nas.py
│ └─ filler.py
│
├─ snapshots/
│ ├─ 1909/ 
│ │ └─ timestamp.csv
│ │
│ ├─ 2023/
│ │ └─ timestamp.csv
│ │
│ └─ 3008/
│ └─ timestamp.csv
│
├─ .gitignore
├─ README.md
└─ requirements.txt
```

## 🔩 Module Descriptions

### Monitor
Contains workflow management files including email alert automation and scrape runners

#### __init__.py
- marks monitor directory as a package, allowing esier imports across the project

#### diff_utils.py
- provides utilities to compare the newest and previous data snapshots
- normalizes CSV data, detects added, removed, and modified records
- writes easy-to-share diff reports

#### notify.py
- provides notification utilities for weekly workflow
- send plain-text email alerts through SMTP connection and can post simple messages to Discord channel using webhook

#### orchestrator.py - (Under construction)
- manages weekly workflow for the projecct
- creates required folders, cofnigures logging, runs full pipeline including data scraping and snapshot comparison and report email
- also captures successes and failures in timepstamped log files

#### run_all.py
- manages weekly workflow for the projecct
- creates required folders, cofnigures logging, runs full pipeline including data scraping and snapshot comparison and report email
- also captures successes and failures in timepstamped log files

#### send_email.py - (Under Construction)
- handles email sending notifications 
- loads credentials from .env file
- connects to outlook SMTP server to send formatted weekly change reports

#### test_runner.py
- lightweight diffing pipeline tester without running full scrapes
- compares lates two snapshots of some specific award ID
- computes and writes CSV diffs and logs summary to file and console

### Scrapers
Contains all scrape scripts

#### __init__.py
- marks scraper directory as a package, allowing esier imports across the project

#### nae.py
- script for National Academy of Engineers website scrape

#### nam.py
- script for National Academy of Medicine website scrape

#### nas.py
- script for National Academy of Science website scrape

### Snapshots
- Contains all CSV files of logged additions, removals, and modifications caught by diff_utils.py and scrape scripts

#### 1909
- NAM CSV files

#### 2023
- NAS CSV Files

#### 3008
- NAE CSV files

### Requirements.txt
- Lists all Python dependencies needed to run the project

