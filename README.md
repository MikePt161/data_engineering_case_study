# Data Engineering Case Study

## Description

The current repository serves as the produced output of the tasks demonstrated in the exercise.md file. Each script within the src/ folder is linked to a directory within the output/ folder with the same name.
## Setup

To run this version the following requirements must be met on a local PC.

- Install Python 3.10
- Install pyspark 3.5.0
- Install packages within requirements.txt
- Additionally setup Hadoop v3.3.5 within a Windows operating system, by accordingly specifying the HADOOP_HOME environment variable.

## Project Outline

```bash
├───.github 
│   └───workflows : CI/CD build pipelines.
├───archive : Archived documents.
│   └───python : Archived / Non production ready python code.
├───data : Source csv files containing necessary provided data.
├───output : Hosts directories that include csv files, containing extracted data per task.
│   ├───best_salesperson
│   ├───department_breakdown
│   ├───it_data
│   ├───marketing_address_info
│   ├───top_3
│   └───top_3_most_sold_per_department_netherlands
├───src : Source code, namely python scripts that produce extracted data, saved in the outputs/ folder.
├───utility: Test functions and logging configuration files.
```
