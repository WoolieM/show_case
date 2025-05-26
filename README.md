# SSIS Data Lineage Extractor (Demonstration Project)

This Python project serves as a demonstration of my skills in data engineering, specifically in extracting data lineage from SQL Server Integration Services (SSIS) packages and SQL Server stored procedures. It showcases my ability to work with XML parsing, database connections, cloud storage interaction (Azure Blob), and data manipulation using Python libraries.

**Please Note:** This repository is intended as a showcase of my technical abilities for professional reference (e.g., for my resume and portfolio). I am not accepting external contributions, modifications, or pull requests to this project.

## Overview
This project illustrates the process of programmatically analyzing data integration workflows to understand the relationships between data sources and targets. It focuses on extracting lineage information from SSIS `.dtsx` files and SQL Server stored procedures, and demonstrates connectivity to Azure Blob Storage.

## Features (Demonstrated)

- **SSIS Package Analysis:** Demonstrates parsing of `.dtsx` files using XML to identify parent and child tables.
- **Stored Procedure Analysis:** Shows how to connect to SQL Server and extract table usage within stored procedure definitions.
- **Azure Blob Storage Connection:** Illustrates connecting to and interacting with Azure Blob Storage using Python.
- **Data Wrangling with Pandas:** Highlights the use of pandas for structuring and presenting the extracted data lineage.
- **Output:** Generates a data lineage map (primarily for demonstration purposes).

## Technologies Used (Demonstrated)

- Python
- `xml.etree.ElementTree`
- `pyodbc`
- `azure-storage-blob`
- `fsspec` (with `AzureBlobFileSystem`)
- `pandas`

## Setup and Usage (For Demonstration Purposes)

This section provides a general idea of how the project functions. If you are reviewing this for reference, please note that specific setup and execution details are for demonstration and might not be fully documented for public use.

- **Environment:** This project was developed using Python 3.x with the libraries listed above.
- **Configuration:** Connection details for SQL Server and Azure Blob Storage are managed within the project's scripts (not intended for public configuration in this demonstration).
- **Execution:** The main Python scripts can be run to perform the lineage extraction and generate the output.

## Contributing

As stated above, this repository is a personal demonstration project and I am not accepting external contributions, modifications, or pull requests.

## License

This project is provided as a demonstration of my skills and is not intended for public use or distribution as a library or application. The code is provided "as-is" for illustrative purposes. Any included third-party libraries are subject to their respective open-source licenses, which are acknowledged in a separate LICENSE file.

## Acknowledgements

This project utilizes the following excellent open-source libraries for demonstration purposes:

- Python
- `aioodbc`
- `asttokens`
- `comm`
- `debugpy`
- `decorator`
- `executing`
- `greenlet`
- `iniconfig`
- `ipykernel`
- `ipython`
- `jedi`
- `jupyter_client`
- `jupyter_core`
- `matplotlib-inline`
- `nest-asyncio`
- `numpy`
- `packaging`
- `pandas`
- `parso`
- `pexpect`
- `platformdirs`
- `pluggy`
- `prompt_toolkit`
- `psutil`
- `ptyprocess`
- `pure_eval`
- `Pygments`
- `pyodbc`
- `pytest`
- `pytest-asyncio`
- `python-dateutil`
- `pytz`
- `PyYAML`
- `pyzmq`
- `six`
- `SQLAlchemy`
- `stack-data`
- `tornado`
- `traitlets`
- `typing_extensions`
- `tzdata`
- `wcwidth`
