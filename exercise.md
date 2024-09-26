# Programming Exercise using PySpark

## Table of Contents

- [Technical information](#technical-information)
    - [Technical information Bonus](#technical-information-bonus)
    - [Technical information Hard Bonus](#technical-information-hard-bonus)
    - [Copilot/ChatGPT, etc. Information](#copilotchatgpt-etc-information)
- [Background](#background)
- [What is wanted?](#what-is-wanted)
    - [Output #1 - IT Data](#output-1---it-data)
    - [Output #2 - Marketing Address Information](#output-2---marketing-address-information)
    - [Output #3 - Department Breakdown](#output-3---department-breakdown)
- [What is wanted? Bonus](#what-is-wanted-bonus)
    - [Output #4 - Top 3 best performers per department](#output-4---top-3-best-performers-per-department)
    - [Output #5 - Top 3 most sold products per department in the Netherlands](#output-5---top-3-most-sold-products-per-department-in-the-netherlands)
    - [Output #6 - Who is the best overall salesperson per country](#output-6---who-is-the-best-overall-salesperson-per-country)
- [What is wanted? Extra Bonus](#what-is-wanted-extra-bonus)

## Technical information:

- Use **Python** version **3.10**
- Use **PySpark** version **3.5.0**
- Use the following package for PySpark tests - https://github.com/MrPowers/chispa - the application **needs** to have tests.
- Avoid using notebooks, like **Jupyter** for instance. While these are good for interactive work and/or prototyping in this case they shouldn't be used.
- Do not use **Pandas**. You can make use of **Pandas** within **PySpark** but do not use it standalone.
- The project should be stored in **GitHub**, if possible in a private repository and you should only commit relevant files to the repository.
- If possible, have different branches for different tasks that once completed are merged to the main branch. Follow the GitHub flow - https://guides.github.com/introduction/flow/.
- Add a **README** file explaining on a high level what the application does.
- Use **logging** to show information about what the application is doing, avoid using **print** statements, but feel free to use them in the tests for your own debugging purposes. However, they should not be there in the final version.
- Application should receive at least **two arguments** which are the paths to each of the dataset files.
- Requirements file should exist.
- Document the code with docstrings as much as possible using the reStructuredText (reST) format.

### Technical information Bonus

- Type hints.
- If possible, the application should have an automated build pipeline using GitHub Actions - https://docs.github.com/en/actions - or Travis - https://www.travis-ci.com/ for instance.
- If possible log to a file with a rotating policy.
- Code should be able to be packaged into a source distribution file.

### Technical information Hard Bonus

- Make use of linters, code-formatters, etc. and have it the Continuous Integration step of your automated build.
- Make use of mypy and/or pydantic.
- Use pre-commit with the hooks that you feel make the most sense.
- Have an entry point for the application called **sales-data**.

### Copilot/ChatGPT, etc. Information

- **You are free to make use of these tools. However, be aware that you should be able to explain what the code you have written does in a satisfactory manner.**

## Background:
A very small company called **EternalTeleSales Fran van Seb Group** specialized in telemarketing for different areas wants to get some insights from their employees. However there are three separate datasets.
- The file **dataset_one.csv** has information about the area of expertise of an employee and the number of calls that were made and also calls that resulted in a sale.
- The file **dataset_two.csv** has personal and sales information, like **name**, **address** and **sales_amount**.
- The file **dataset_three.csv** has data about the sales made, which company the call was made to where the company is located, the product and quantity sold. The field **caller_id** matches the ids of the other two datasets.

## What is wanted?

### Output #1 - **IT Data**

The management teams wants some specific information about the people that are working in selling IT products.

- Join the two datasets.
- Filter the data on the **IT** department.
- Order the data by the sales amount, biggest should come first.
- Save only the first **100** records.
- The output directory should be called **it_data** and you must use PySpark to save only to one **CSV** file.

### Output #2 - **Marketing Address Information**

The management team wants to send some presents to team members that are work only selling **Marketing** products and wants a list of only addresses and zip code, but the zip code needs to be in it's own column.

- The output directory should be called **marketing_address_info** and you must use PySpark to save only to one **CSV** file.

### Output #3 - **Department Breakdown**

The stakeholders want to have a breakdown of the sales amount of each department and they also want to see the total percentage of calls_succesfful/calls_made per department. The amount of money and percentage should be easily readable.

- The output directory should be called **department_breakdown** and you must use PySpark to save only to one **CSV** file.

## What is wanted? Bonus

### Output #4 - **Top 3 best performers per department**

The management team wants to reward it's best employees with a bonus and therefore it wants to know the name of the top 3 best performers per department. That is the ones that have a percentage of calls_succesfful/calls_made higher than 75%. It also wants to know the sales amount of these employees to see who best deserves the bonus. In your opinion, who should get it and why?

- The output directory should be called **top_3** and you must use PySpark to save only to one **CSV** file.

### Output #5 - **Top 3 most sold products per department in the Netherlands**

- The output directory should be called **top_3_most_sold_per_department_netherlands** and you must use PySpark to save only to one **CSV** file.

### Output #6 - **Who is the best overall salesperson per country**

- The output directory should be called **best_salesperson** and you must use PySpark to save only to one **CSV** file.

## What is wanted? Extra Bonus

- Please derive other two insights from the three datasets that you find interesting. Either create new datasets in **CSV** or if you prefer create some graphs.

- Please save them as **extra_insight_one** and **extra_insight_two** directories and if you create a dataset you must use PySpark to save only to one **CSV** file.
