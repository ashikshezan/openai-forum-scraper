# OpenAI Forum Scraper

## Project Overview

This Scrapy project is designed to scrape topics from the OpenAI Community Forum. It fetches topic details based on a specified number of past days and provides the option to save the scraped data either in a PostgreSQL database or as a JSON file.

## Features

- Scrape topics from OpenAI Community Forum.
- Filter topics based on a specified date range (number of past days).
- Save scraped data in a PostgreSQL database or as a JSON file.
- Batch insertion for database efficiency.
- Customizable output method via command-line arguments.
- Configuration using environment variables for enhanced security and flexibility.

## Requirements

- Python 3.10+
- Scrapy
- psycopg2 (for PostgreSQL database interaction)
- python-dotenv (for environment variable management)

## Installation

Install dependencies:

```
pip install -r requirements.txt
```

## Environment Configuration

1. Create a `.env` file in the root of your project.
2. Add your PostgreSQL database configuration and other environment variables to the `.env` file:
3. The spider will automatically create the necessary table (`topic_details`) in the specified database.

```
POSTGRES_URI=your_postgres_host
POSTGRES_USER=your_username
POSTGRES_PASS=your_password
POSTGRES_DB=your_database
```

## Usage

The spider can be run with the following command, where `output_method` can be either `json` or `postgres`, and `days` is an integer representing the number of past days to scrape:

```
scrapy crawl openai_forum -a output_method=json -a days=7
or
scrapy crawl openai_forum -a output_method=postgres -a days=7
```

### JSON Output

If `output_method=json` is used, the scraped data will be saved in a file named `output.json` in the project directory.

### PostgreSQL Output

If `output_method=postgres` is selected, ensure that your PostgreSQL server is running and accessible. You will need to configure the database settings in `settings.py` or pass them through environment variables.

Customization

You can customize the spider and pipeline according to your needs. The project is structured to allow easy modifications and extensions.
