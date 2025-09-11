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


