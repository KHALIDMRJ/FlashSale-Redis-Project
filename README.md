⚡ FlashSale Redis Project
High-Performance Flash Sale System using Redis & Concurrency Control










🧠 Abstract

The FlashSale Redis Project is a distributed flash sale simulation system designed to evaluate the performance and reliability of Redis-based concurrency control mechanisms under high request loads.

It models a real-world flash sale environment where thousands of users attempt to purchase limited stock simultaneously.

Redis is used as a central in-memory datastore to:

Guarantee atomic operations

Prevent race conditions

Maintain data consistency

Achieve low-latency responses

This project integrates concepts from:

Distributed systems

Concurrent programming

Performance benchmarking

Software engineering documentation

🎯 Objectives

Design a scalable flash sale service architecture

Implement concurrency-safe stock management using Redis

Simulate multiple concurrent clients

Measure system behavior under heavy load

Analyze performance results

Provide complete academic documentation

🏗️ System Architecture

The system is composed of three logical layers:

1️⃣ Service Layer

Handles product stock management

Implements business logic

Communicates with Redis

Performs atomic stock updates

Manages logging and errors

2️⃣ Simulation Layer

Generates concurrent client requests

Stress-tests the service

Records success/failure ratios

Measures execution time

3️⃣ Reporting & Analysis Layer

Aggregates logs

Produces charts and figures

Documents results

Provides a final technical report

Redis guarantees atomicity and isolation for shared resources.

🛠️ Technologies & Tools
Category	Technology
Programming Language	Python 3
Database	Redis (In-Memory)
Containerization	Docker, Docker Compose
Concurrency	Multithreading / multiprocessing
Version Control	Git & GitHub
Documentation	Markdown
Visualization	PNG / PDF
📁 Project Structure
FlashSale-Redis-Project/
│
├── 1_Service/                 # Core backend service
│   ├── app.py                 # Main service entry point
│   ├── redis_client.py        # Redis connection & atomic operations
│   └── config.py              # Configuration parameters
│
├── 2_Simulation/              # Client load simulation
│   ├── simulate_clients.py    # Concurrent client generator
│   ├── scenario_concurrency.py# Load scenarios
│   └── results_logs.txt       # Execution logs
│
├── 3_Report/                  # Experimental results
│   ├── FlashSale_Project_Report.pdf
│   └── figures/               # Charts & architecture visuals
│
├── 4_Installation/            # Setup & deployment
│   ├── docker-compose.yml
│   ├── requirements.txt
│   └── README.md
│
├── 5_Gantt/                   # Project planning
│   └── gantt_chart.png
│
├── 6_Annexes/                 # Supplementary materials
│   └── jira_board_screenshots/
│
└── README.md                  # Main documentation

⚙️ Installation & Setup
🔹 Prerequisites

Python 3.x

Docker & Docker Compose

Git

🔹 Clone the Repository
git clone https://github.com/KHALIDMRJ/FlashSale-Redis-Project.git
cd FlashSale-Redis-Project

🔹 Install Dependencies
pip install -r 4_Installation/requirements.txt

🔹 Start Redis with Docker
docker-compose up -d

▶️ Execution Workflow
Step 1: Start the Service
python 1_Service/app.py


Initializes:

Redis connection

Stock values

Service logic

Logging system

Step 2: Run Simulation
python 2_Simulation/simulate_clients.py


The simulation:

Spawns concurrent clients

Sends purchase requests

Logs success and failure

Measures performance

🔐 Concurrency Control Strategy

Redis atomic operations are used to ensure thread-safe stock management.

Core Algorithm
stock = redis.decr("product_stock")

if stock >= 0:
    return "SUCCESS"
else:
    redis.incr("product_stock")
    return "FAILED"


Guarantees:

No overselling

Atomic execution

High throughput

Lock-free synchronization

🧠 Redis Commands Used
Command	Purpose
GET	Retrieve stock
SET	Initialize stock
DECR	Atomic decrement
INCR	Rollback operation
MULTI/EXEC	Transaction blocks
PING	Health check
FLUSHALL	Reset environment
🧵 Simulation Model

Each client sends one purchase request

Requests run concurrently

Response times measured

Results logged in results_logs.txt

Parameters:

Number of clients (N)

Initial stock (S)

Request rate (R)

Execution time (T)

📊 Performance Metrics

Metrics analyzed:

Throughput (requests/second)

Average response time

Success vs failure ratio

Stock consistency

Redis latency

Formula:

Throughput = Total Requests / Execution Time

🧪 Experimental Scenarios

Defined in:

2_Simulation/scenario_concurrency.py


Scenarios:

Low concurrency (10–50 clients)

Medium concurrency (100–500 clients)

High concurrency (1000+ clients)

Each scenario evaluates:

Stability

Consistency

Performance degradation

🔄 Fault Tolerance Strategy

Redis is the single source of truth

Atomic rollback when stock < 0

Logging of failed operations

Graceful error handling

🧩 Design Patterns Used

Layered Architecture

Client–Server Model

Centralized State Management

Atomic Transaction Pattern

Producer–Consumer (simulation)

🧬 Data Model (Redis Keys)
product:{id}:stock → integer
logs:{timestamp} → string
client:{id}:status → SUCCESS / FAILED

🔍 Complexity Analysis
Operation	Complexity
Redis GET	O(1)
Redis DECR	O(1)
Request handling	O(1)
Simulation loop	O(n clients)
🧩 UML Diagrams
📌 Use Case Diagram
Diagramme
graph TD
    User[Client/User] --> Simulation[Simulation Layer]
    Simulation --> Service[Flash Sale Service]
    Service --> Redis[(Redis Database)]
    Service --> User

📌 Class Diagram
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

📌 Sequence Diagram
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

🎓 Learning Outcomes

This project demonstrates:

Redis atomicity and synchronization

Flash sale system design

Concurrent programming

Distributed data consistency

Performance benchmarking

UML modeling

Professional documentation

👨‍💻 Author

Khalid Morjane
University Project – Flash Sale System using Redis

GitHub: https://github.com/KHALIDMRJ

📜 License

This project is developed strictly for academic and educational purposes.
