# multinational-retail-data-centralisation481

## Overview
This project addresses the challenge faced by a multinational company that sells various goods globally. The company's sales data is currently dispersed across multiple sources, making it difficult to access and analyze.

## Problem
The scattered nature of sales data prevents team members from easily accessing and analyzing it, hindering the organization's ability to be data-driven.

### Solution
  1. Centralized Database: Develop a system to store all current sales data in a centralized database, ensuring a single source of truth for sales information.
  2. Data Querying: Implement queries to extract up-to-date business metrics from the centralized database.
This project will streamline data accessibility and analysis, enabling the organization to make informed, data-driven decisions.

#### File Structure

/Your_Repository
│
├── README.md
│   └── Update README.md
│
├── Schema
│   ├── star_schema
│   │   ├── keys.sql
│   │   ├── dim_card_details.sql
│   │   ├── dim_date_times.sql
│   │   ├── dim_products.sql
│   │   ├── dim_store_details.sql
│   │   ├── dim_users.sql
│   │   └── orders_table.sql
│
├── main.py
├── data_cleaning.py
├── data_extraction.py
├── database_utils.py
└── Database_query_script.sql
    └── Task 4 questions
