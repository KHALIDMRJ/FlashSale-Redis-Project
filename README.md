# ⚡ FlashSale Redis Project
### High-Performance Flash Sale System using Redis & Concurrency Control  

![Python](https://img.shields.io/badge/Python-3.x-blue.svg)
![Redis](https://img.shields.io/badge/Redis-InMemory-red.svg)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue.svg)
![Status](https://img.shields.io/badge/Status-Academic%20Project-success.svg)
![License](https://img.shields.io/badge/License-Educational-lightgrey.svg)

---

## 🧠 Abstract

The **FlashSale Redis Project** is a distributed flash sale simulation system designed to evaluate the performance and reliability of **Redis-based concurrency control mechanisms** under high request loads.  

It models a real-world flash sale environment where thousands of users attempt to purchase limited stock simultaneously.

Redis is used as a central in-memory datastore to:
- Guarantee atomic operations  
- Prevent race conditions  
- Maintain data consistency  
- Achieve low-latency responses  

This project combines distributed systems, concurrency control, and performance benchmarking.

---

## 🎯 Objectives

- Design a scalable flash sale service architecture  
- Implement concurrency-safe stock management using Redis  
- Simulate multiple concurrent clients  
- Measure system behavior under heavy load  
- Analyze performance results  
- Provide a complete academic report  

---

## 🏗️ System Architecture

The system is organized in **three main layers**:

1️⃣ **Service Layer** – manages business logic and Redis interactions  
2️⃣ **Simulation Layer** – generates concurrent clients for load testing  
3️⃣ **Reporting Layer** – aggregates logs, produces charts, and documents results  

Redis guarantees **atomicity and isolation**, ensuring no overselling or race conditions.

---

## 📁 Project Structure

```text
FlashSale-Redis-Project/
├── 1_Service/                  # Core backend service
│   ├── app.py                  # Main service entry point
│   ├── redis_client.py         # Redis connection & atomic operations
│   └── config.py               # Configuration parameters
│
├── 2_Simulation/               # Client load simulation
│   ├── simulate_clients.py     # Concurrent client generator
│   ├── scenario_concurrency.py # Load scenario definitions
│   └── results_logs.txt        # Simulation logs
│
├── 3_Report/                   # Experimental results
│   ├── FlashSale_Project_Report.pdf
│   └── figures/                # Charts, diagrams, architecture visuals
│
├── 4_Installation/             # Deployment & setup
│   ├── docker-compose.yml
│   ├── requirements.txt
│   └── README.md
│
├── 5_Gantt/                    # Project planning
│   └── gantt_chart.png
│
├── 6_Annexes/                  # Supplementary materials
│   └── jira_board_screenshots/
│
└── README.md                   # Main documentation
```

## ⚙️ Installation & Setup
Prerequisites

Python 3.x

Docker & Docker Compose

Git

Clone Repository
git clone https://github.com/KHALIDMRJ/FlashSale-Redis-Project.git
cd FlashSale-Redis-Project

Install Dependencies
pip install -r 4_Installation/requirements.txt

Start Redis
docker-compose up -d

## ▶️ Execution Workflow
Step 1 – Start Service
python 1_Service/app.py


Initializes:

Redis connection

Stock values

Service logic

Logging

Step 2 – Run Simulation
python 2_Simulation/simulate_clients.py


Simulation:

Spawns concurrent clients

Sends purchase requests

Logs success/failure

Measures performance

## 🔐 Concurrency Control Strategy

Redis atomic operations ensure thread-safe stock management:

stock = redis.decr("product_stock")

if stock >= 0:
    return "SUCCESS"
else:
    redis.incr("product_stock")
    return "FAILED"


Guarantees:

No overselling

Atomic execution

Lock-free concurrency

High throughput

## 🧠 Redis Commands Used
Command	Purpose

GET	Retrieve stock

SET	Initialize stock

DECR	Atomic decrement

INCR	Rollback operation

MULTI/EXEC	Transaction blocks

PING	Health check

FLUSHALL	Reset environment
## 🧵 Simulation Model

Each client sends one purchase request

Requests run concurrently

Response times logged

Results saved in results_logs.txt

Parameters:

Number of clients (N)

Initial stock (S)

Request rate (R)

Execution time (T)

## 📊 Performance Metrics

Metrics:

Throughput (req/s)

Average response time

Success vs failure ratio

Stock consistency

Redis latency

Formula:

Throughput = Total Requests / Execution Time

## 🧩 UML Diagrams
Use Case Diagram
Diagramme
graph TD
    User[Client/User] --> Simulation[Simulation Layer]
    Simulation --> Service[Flash Sale Service]
    Service --> Redis[(Redis Database)]
    Service --> User

Class Diagram
Diagramme
classDiagram
    class AppService {
        +start_service()
        +process_request()
        +update_stock()
    }

    class RedisClient {
        +connect()
        +get_stock()
        +decrement_stock()
        +set_stock()
    }

    class SimulationClient {
        +run_simulation()
        +send_request()
    }

    AppService --> RedisClient
    SimulationClient --> AppService

Sequence Diagram
Diagramme
sequenceDiagram
    participant Client
    participant Service
    participant Redis

    Client->>Service: purchase(product_id)
    Service->>Redis: DECR stock
    Redis-->>Service: updated stock
    alt stock >= 0
        Service-->>Client: SUCCESS
    else stock < 0
        Service->>Redis: INCR (rollback)
        Service-->>Client: FAILED
    end

## 🎓 Learning Outcomes

Redis atomicity & synchronization

Flash sale system design

Concurrent programming

Distributed data consistency

Performance benchmarking

UML modeling

Professional documentation

## 👨‍💻 Author

Khalid Morjane

Anas Lahraoui

University Project – Flash Sale System using Redis

GitHub: https://github.com/KHALIDMRJ

GitHub: https://github.com/anabkl

📜 License

This project is developed strictly for academic and educational purposes.
