# DBT - SNOWFLAKE Project - Developer Setup Instructions

**A special thank you to the YouTube channel 'Data With Baraa' for the inspiration behind this project!** This work was particularly inspired by the excellent video tutorial: [Dimensional Data Warehouse Tutorial](https://youtu.be/9GVqKuTVANE?si=XEs0sgFyHvx6fHtA), which is very well designed for junior and medium data engineers. The tutorial provides a comprehensive introduction to dimensional modeling and data warehouse concepts. You can check out more content from Baraa on his website: [https://www.blog.datawithbaraa.com/p/access-to-course-materials](https://www.blog.datawithbaraa.com/p/access-to-course-materials) and explore the original project on [GitHub](https://github.com/DataWithBaraa/sql-data-warehouse-project/tree/main).

## Key Differences in My Implementation

While following the concepts from the original tutorial, my implementation differs in several ways:
- **Technology Stack**: Instead of using SQL Server entirely, I used Snowflake as the data warehouse solution combined with dbt for handling ingestion and transformations.
- **Containerization**: The project is containerized so that anyone can start working on it knowing that all necessary libraries and versions are already included.
- **Ingestion Method**: As this is a dbt project, I use seeds for data ingestion (explained in the Setup Instructions below).
- **Architecture**: I implemented a SEEDS schema where data is loaded from CSV files, with the LANDING schema serving as the equivalent of the bronze layer from the video.
- **Layer Naming**: While still following the medallion architecture, I named the schemas and layers as landing, staging, mart, and reporting to better reflect how a production project would look like.

## Prerequisites

Before starting, ensure you have the following installed on your local machine:
- **Docker Desktop** (for containerization) - Download from [Docker's official website](https://www.docker.com/products/docker-desktop/)
- **VSCode** with the "Dev Containers" extension
- **Git** (for version control)

**Important**: Make sure Docker Desktop is running before attempting to open the project in a container.

### Check if Git is installed and configured:

```bash
git --version
```

```bash
git config --global user.name
```

```bash
git config --global user.email
```

If Git is not installed or configured, follow these steps:

**Install Git:**
- **Windows**: Download from [git-scm.com](https://git-scm.com/download/win)
- **macOS**: `brew install git` or download from [git-scm.com](https://git-scm.com/download/mac)
- **Linux**: `sudo apt-get install git` (Ubuntu/Debian) or equivalent for your distribution

**Configure Git:**

```bash
git config --global user.name "Your Name"
```

```bash
git config --global user.email "your.email@booking.com"
```

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/sant3e/dbt_snowflake_dwh_project.git
```

```bash
cd dbt_snowflake_dwh_project
```

**Note**: You can also clone using SSH if you have SSH keys set up with GitHub:

```bash
git clone git@github.com:sant3e/dbt_snowflake_dwh_project.git
```

### 2. Open in VSCode and Setup Container

```bash
code .
```

- VSCode will detect the `.devcontainer` configuration
- Click "Reopen in Container" when prompted
- VSCode will automatically build the container and install all extensions
- Wait for the container to build (this may take a few minutes on first run)


### 3. Configure Your Snowflake Credentials

**This is the most critical step!** Copy the example environment file and edit it with your actual Snowflake credentials:

```bash
cp .env.example .env
```

Then open and edit the `.env` file with your actual Snowflake credentials:

```bash
nano .env
```

If you don't have nano installed, just open the file in VSC.
Read the `.env.example` file carefully and replace the placeholder values with your actual Snowflake information. For this project, use password authentication by uncommenting and filling in the `SF_USER_PASSWORD` value.


**Important Notes:**
- Your `.env` file contains sensitive credentials - never commit it to Git
- Ask your team lead for the correct values for your environment

### 4. Set the Correct Target in profiles.yml

Edit the target setting in `dbt_project/profiles.yml` to match your chosen authentication method:

```bash
nano dbt_project/profiles.yml
```

Change the `target:` line at the top to:
- `target: local_password` (for password auth)

### 5. Install DBT Packages

```bash
cd /app/dbt_project
```

```bash
dbt deps
```

This installs all required dbt packages including:
- dbt_utils (utility macros)
- dbt_expectations (data quality tests)
- codegen (code generation helpers)
- codegen (code generation helpers)

### 6. Test Your Connection

```bash
dbt debug
```

**Expected Output:**
```
Configuration:
    profiles.yml file [OK found and valid]
    dbt_project.yml file [OK found and valid]

Required dependencies:
    - git [OK found]

Connection:
    account: your_account
    user: your_username
    database: your_database
    schema: your_schema
    warehouse: your_warehouse
    role: your_role
    All checks passed!
```

**If you see errors:**
- Double-check your `.env` file values
- Verify your Snowflake access permissions
- Ensure the target in `profiles.yml` matches your authentication method
- Ask your team lead for help with Snowflake credentials

### 7. Initialize Snowflake Resources

Before running dbt models, you'll need to set up your Snowflake database and schemas. This project requires a free Snowflake trial account first.

Open the file `scripts/initial_snowflake_setup.sql` in this project, copy the SQL commands, and run them directly in a Snowflake worksheet (Snowsight) to create the required warehouse, database, and schemas.

### 8. Data Ingestion with Seeds

In this dbt project, we use seeds for data ingestion from CSV files. Seeds are dbt's functionality for loading data from external CSV or TSV files directly into database tables. 

**Ingestion Process:**
- Place your CSV files in the `dbt_project/seeds/` directory
- When you run `dbt seed`, dbt will automatically create the SEEDS schema in Snowflake (if it doesn't exist) and load your CSV files as tables
- The LANDING schema contains models that reference these seed tables as the bronze layer
- From there, the data flows through the staging and mart layers following dimensional modeling principles

**To load seed data, run the following command:**
```bash
dbt seed
```

This command will read all CSV files from the `seeds/` directory and create corresponding tables in your target schema.

### 9. Start Developing

```bash
# Run all models - automatically creates schemas and collects metadata
dbt run
```

```bash
# Run all models
dbt run
```

```bash
# Compile models without running them
dbt compile
```

```bash
# Generate documentation
dbt docs generate
```

```bash
# Serve documentation locally
dbt docs serve
```

## Project Structure

Once set up, you'll be working with this structure:

```
project_name/
├── .devcontainer/ (development container config)
├── dbt_project/ (main dbt project)
│   ├── analyses/ (analysis files)
│   ├── dbt_packages/ (installed dbt packages)
│   ├── logs/ (dbt logs)
│   ├── macros/ (custom SQL functions)
│   ├── models/
│   │   ├── landing/ (raw data from sources)
│   │   ├── mart/ (dimensional models)
│   │   ├── staging/ (transformed source data)
│   │   └── reporting/ (business-facing reports)
│   ├── packages.yml (dbt dependencies)
│   ├── profiles.yml (Snowflake connections)
│   ├── seeds/ (seed data files)
│   ├── snapshots/ (historical data tracking)
│   ├── sources.yml (source table definitions)
│   ├── target/ (compiled dbt artifacts)
│   ├── tests/ (data quality tests)
│   └── dbt_project.yml (main project config)
├── logs/ (application logs)
├── scripts/ (utility scripts)
├── .dockerignore
├── .env (your personal credentials - not in Git)
├── .env.example (template for environment variables)
├── .gitignore
├── Dockerfile
├── README.md
├── requirements.txt
└── test_connection.py
```

## Available VSCode Extensions

The dev container automatically installs these helpful extensions:
- **dbt Extensions**: Syntax highlighting, formatting, shortcuts
- **dbt Power User**: Advanced dbt development features
- **Python Extension Pack**: For Python models and analysis
- **Jupyter**: For data exploration notebooks
- **Better Jinja**: Enhanced Jinja template support
- **vscode-altimate-mcp-server**: Datamates functionality support

## Common Commands

### Development Workflow
```bash
# Work on specific models
dbt run --select model_name
dbt run --select staging.*
dbt run --select marts.dim_table_name+

# Test specific models
dbt test --select model_name
dbt test --select staging.*

# Fresh start (clean and rebuild)
dbt clean
dbt deps
dbt run
```



### Documentation
```bash
# Generate and serve docs
dbt docs generate
dbt docs serve --port 8001
```

## Troubleshooting

### Connection Issues
1. **"Could not connect to Snowflake"**
     - Verify your `.env` file has correct values
     - Check that your Snowflake user is active
     - Ensure your role has proper permissions

2. **"Database/Schema does not exist"**
     - Ask your team lead to verify your database access
     - Check if you're using the correct database name in `.env`

3. **"Authentication failed"**
     - For OAuth: verify your account identifier, username and password are correct

### SSH/Git Issues
```bash
# Test SSH connection to GitLab
ssh -T git@gitlab.com

# If SSH fails, try HTTPS with Personal Access Token
git remote set-url origin https://gitlab.com/path/to/repo.git
```

### Container Issues
- If the container won't start: Try "Dev Containers: Rebuild Container" in VSCode
- If extensions aren't loading: Check the "Extensions" tab in VSCode
- If you get permission errors: Ensure Docker Desktop is running

## Getting Help

1. **Check the logs**: Look in `dbt_project/logs/` for detailed error messages
2. **Ask your team**: Your team lead can help with Snowflake credentials and permissions
3. **Use Cody AI**: The AI assistant can help with dbt syntax and best practices
4. **DBT Documentation**: [docs.getdbt.com](https://docs.getdbt.com/)

## Next Steps

After successful setup:
1. **Extend with more models** in `models/reporting/`
2. **Review the documentation** by running `dbt docs serve`
3. **Check out the sources** defined in `models/sources.yml`
4. **Understand the project structure**
5. **Ask questions** - your team is there to help!