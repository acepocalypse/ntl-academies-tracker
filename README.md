# National Academy Tracker
**Authors:** Ace Setiawan & Ana Farmus  
**Date:** September 2025

## Overview
This program automates web-scrapes of the **National Academy of Engineering**, **Medicine**, and **Science** websites.  
It logs **additions**, **removals**, and **modifications** to each site in `.csv` files.  

- Runs **weekly** on a dedicated server or computer.  
- Sends an **email alert** to the Purdue EDA team with links to the CSV reports showing changes.

---

## Directory Overview

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

## Module Descriptions

### Monitor  
Contains workflow management files including email alert automation and scrape runners

#### __init__.py
- marks Monitor directory as a package, allowing esier imports across the project

#### diff_utils.py
- provides utilities to compare the newest and previous data snapshots
- normalizes CSV data, detects added, removed, and modified records
- writes easy-to-share diff reports

#### notify.py
- provides notification utilities for weekly workflow
- send plain-text email alerts through SMTP connection and can post simple messages to Discord channel using webhook

#### run_all.py
- manages weekly workflow for the projecct
- creates required folders, cofnigures logging, runs full pipeline including data scraping and snapshot comparison and report email
- also captures successes and failures in timepstamped log files

#### test_runner.py
- lightweight diffing pipeline tester without running full scrapes
- compares lates two snapshots of some specific award ID
- computes and writes CSV diffs and logs summary to file and console

### Scrapers  
Contains all scrape scripts

#### __init__.py
- marks Scrapers directory as a package, allowing esier imports across the project

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

### .gitignore
- Hides sensitive information

### Requirements.txt
- Lists all Python dependencies needed to run the project


## Steps for Deployment on New Windows Machine

1. Ensure Python dependencies are installed (see requirements.txt). Pay special attention to where the Python file is stored, especially the Python.exe file it contains. 
3. Clone repository on local device.
2. Configure local repository to contain any .env files/other hidden files needed to contain email credentials.
4. Open Windows Start and search for Task Manager. Once opened, type Windows key + R. This should open the Run dialogue.
5. In the Run dialogue, enter "taskschd.msc". This will open Task Scheduler.
6. In Task Scheduler, locate "Create Basic Task" on the right hand menu. Next, add a relevant name and description.
7. Click "Next," and select the desired interval of repetition. Then, choose the start date and time, and click "Next".
8. Choose "Start a Program". Enter the path name to your Python.exe file under "Program/script." In the Arguments section underneath, type the path to the run_all.py file
        Ex: C:\Users\afarmus\vscode\ntl-academies-tracker\monitor\run_all.py"
        You can also select "Browse", select "Computer" fromn the left menu, from the folders that appear select "Users", select your username, navigate to the IDE folder you used to clone the repository (e.g., .vscode), select the project directory "ntl-academies-tracker", select "monitor", and finally select "run_all". 
9. Click "Next" to review the task settings and click "Finish" if satisfied. 
10. Check the task's properties by clicking on it from the Task Scheduler Library. Ensure the settings allow for execution regardless of whether the user is logged on. 
10. The first time the program runs, Windows will likely prompt you to select the application to run the program from. For this reason, plan to be online at the task's first start time to ensure the program does not idle. 


If you wish to edit, delete, disable, or review your new task, navigate to Task Scheduler using the above steps, find the left hand menu and select "Task Scheduler Library".
You should be able to view the task. Click on it to view various settings tabs located underneath the task list, or right-click to view other actions, including Delete.

## Maintenance
Check email report and included snapshot files weekly, keep Python dependencies updated, and adjust settings as websites evolve.
Future Improvements: Virtual Environment Setup maybe of interest to prevent having to re-install all Python dependencies repeatedly

## License
This project is intended for internal Purdue EDA use.  
Contact the authors for permissions if you wish to adapt it externally.

## Acknowledgements
Developed by Ace Setiawan & Ana Farmus with guidance from the Purdue EDA office.