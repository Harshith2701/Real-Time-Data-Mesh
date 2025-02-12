# Real-Time Data Mesh Project

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)](docker/docker-compose.yml)

A scalable, domain-driven data mesh architecture that enables real-time data processing across multiple business domains. This project implements modern data engineering practices using industry-standard tools and frameworks.

## 🚀 Key Features

- **Real-time Data Capture**: Instantly capture database changes using Debezium
- **Event Streaming**: Reliable message delivery with Apache Kafka
- **Distributed Processing**: Scalable data processing using Apache Spark
- **ACID Compliant Storage**: Reliable storage with Delta Lake
- **Workflow Management**: Automated orchestration using Apache Airflow
- **Data Quality**: Automated testing with Great Expectations

## 🏗️ Architecture

Our architecture follows domain-driven design principles, where each domain owns and manages its data pipeline:

<img width="781" alt="image" src="https://github.com/user-attachments/assets/0d3b7f99-3b73-432d-b8e3-60f2d08a7bff" />

### Data Flow

1. **Source Systems**: Domain-specific databases (PostgreSQL, MySQL, MongoDB)
2. **Change Data Capture**: Debezium captures real-time changes
3. **Message Streaming**: Kafka ensures reliable data delivery
4. **Processing Layer**: Spark processes streaming data
5. **Storage Layer**: Delta Lake provides ACID compliant storage
6. **Orchestration**: Airflow manages workflow dependencies

## 📁 Project Structure

```
data-mesh/
├── domains/                  # Domain-specific implementations
│   └── orders/              # Orders domain
│       └── streaming/       # Streaming components
│           └── orders_streaming.py
├── docker/                  # Container configurations
│   └── docker-compose.yml   # Multi-container setup
├── configs/                 # Configuration files
├── tests/                   # Test suites
└── docs/                    # Documentation
```

## 🚦 Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Java 11+ (for Spark)
- Kafka 2.8+
- Apache Spark 3.x

## 🛠️ Getting Started

1. **Clone the Repository**
   ```bash
   git clone https://github.com/Harshith2701/Real-Time-Data-Mesh.git
   cd Real-Time-Data-Mesh
   ```

2. **Environment Setup**
   ```bash
   # Copy example environment file
   cp .env.example .env
   
   # Edit with your configurations
   vim .env
   ```

3. **Start Services**
   ```bash
   cd docker
   docker-compose up -d
   ```

4. **Verify Installation**
   ```bash
   # Check service health
   docker-compose ps
   
   # View logs
   docker-compose logs -f
   ```

## 💻 Usage

### Setting Up a New Domain

1. Create domain directory structure:
   ```bash
   mkdir -p domains/new_domain/{streaming,configs}
   ```

2. Configure Debezium connector:
   ```json
   {
     "name": "new-domain-connector",
     "config": {
       "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
       "database.hostname": "postgres",
       "database.port": "5432",
       "database.user": "postgres",
       "database.password": "postgres",
       "database.dbname": "mydatabase",
       "database.server.name": "new_domain",
       "table.whitelist": "public.my_table"
     }
   }
   ```

### Monitoring

- Access Airflow UI: http://localhost:8080
- Kafka Topics UI: http://localhost:8000
- Spark UI: http://localhost:4040

## 🔍 Testing

```bash
# Run unit tests
python -m pytest tests/unit

# Run integration tests
python -m pytest tests/integration

# Run specific test suite
python -m pytest tests/unit/test_orders_streaming.py
```
