# Fraud Transaction Detection using Python and Neo4j Graph Database

## Overview

This project is a fraud transaction detection system implemented in Python using Neo4j, a graph database, as the underlying data store. The system is designed to identify potentially fraudulent transactions within a network of financial transactions, allowing organizations to mitigate risks and protect their assets.

## Features

- **Graph Database**: Utilizes Neo4j as the database to model and store transaction data as a graph, enabling efficient querying and analysis.

- **Machine Learning**: Implements machine learning algorithms to detect anomalies and patterns in the transaction data.

- **Scalability**: Designed to handle large volumes of transaction data and scale with the growth of the dataset.

- **User-Friendly Interface**: Provides a user-friendly interface for querying and visualizing transaction data and identified fraud cases.

## Prerequisites

Before running the project, ensure you have the following prerequisites installed:

- Python 3.x
- Neo4j Database (with the Neo4j Python driver)
- Required Python libraries (specified in `requirements.txt`)

## Installation

1. Clone the repository:

   ```
   git clone https://github.com/yourusername/fraud-transaction-detection.git
   ```

2. Install the required Python libraries:

   ```
   pip install -r requirements.txt
   ```

3. Set up Neo4j:

   - Install Neo4j from [https://neo4j.com/download/](https://neo4j.com/download/)
   - Create a Neo4j database and note down the credentials.

4. Configure Neo4j Connection:

   Update the Neo4j database connection details in the `config.py` file:

   ```python
   NEO4J_URI = "bolt://localhost:7687"
   NEO4J_USER = "your_username"
   NEO4J_PASSWORD = "your_password"
   ```

## Usage

1. Load Transaction Data: Use the provided data loading scripts to import transaction data into the Neo4j database.

   ```
   python load_data.py
   ```

2. Run Fraud Detection:

   ```
   python detect_fraud.py
   ```

3. Access the User Interface:

   Open the web-based user interface by navigating to [http://localhost:8080](http://localhost:8080) in your web browser.

## Contributing

Contributions are welcome! Please read the [Contributing Guidelines](CONTRIBUTING.md) for details on how to contribute to this project.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- The Neo4j team for providing a powerful graph database.
- The scikit-learn developers for their machine learning library.
- The Python community for their valuable contributions.
