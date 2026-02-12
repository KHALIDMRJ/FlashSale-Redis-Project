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

This project combines software engineering, distributed systems, and performance evaluation principles.

🎯 Objectives

Design a scalable flash sale service architecture

Implement concurrency-safe stock management using Redis

Simulate multiple concurrent clients

Measure system behavior under heavy load

Analyze performance results and document findings

Provide a complete technical and academic report

🏗️ System Architecture

The architecture follows a layered design:

1️⃣ Service Layer

Responsible for:

Product stock management

Business logic execution

Redis communication

Atomic stock decrement operations

Error handling and logging

2️⃣ Simulation Layer

Responsible for:

Generating concurrent client requests

Stress testing the service

Recording results and response statistics

Modeling realistic flash sale scenarios

3️⃣ Reporting & Analysis Layer

Responsible for:

Aggregating logs

Producing performance figures

Technical documentation

Final academic report

Redis ensures atomicity and isolation of operations through its built-in commands and locking mechanisms.

🛠️ Technologies & Tools
Category	Technology
Programming Language	Python 3
Database	Redis (in-memory)
Containerization	Docker, Docker Compose
Concurrency	Multithreading / multiprocessing
Version Control	Git & GitHub
Documentation	Markdown
Visualization	PNG / PDF reports
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
│   ├── scenario_concurrency.py# Load scenarios definition
│   └── results_logs.txt       # Execution logs
│
├── 3_Report/                  # Experimental results
│   ├── FlashSale_Project_Report.pdf
│   └── figures/               # Charts, diagrams, and architecture visuals
│
├── 4_Installation/            # Deployment & setup
│   ├── docker-compose.yml
│   ├── requirements.txt
│   └── README.md
│
├── 5_Gantt/                   # Project planning
│   └── gantt_chart.png
│
├── 6_Annexes/                 # Supplementary material
│   └── jira_board_screenshots/
│
└── README.md                  # Main documentation

⚙️ Installation & Setup
🔹 Prerequisites

Python 3.x

Docker & Docker Compose

Git

Redis (via Docker)

🔹 Clone Repository
git clone https://github.com/KHALIDMRJ/FlashSale-Redis-Project.git
cd FlashSale-Redis-Project

🔹 Install Python Dependencies
pip install -r 4_Installation/requirements.txt

🔹 Launch Redis using Docker
docker-compose up -d

▶️ Execution Workflow
Step 1: Start the Service
python 1_Service/app.py


This initializes:

Redis connection

Stock values

Service endpoints

Logging system

Step 2: Run Client Simulation
python 2_Simulation/simulate_clients.py


The simulation:

Spawns multiple concurrent clients

Sends purchase requests

Records success/failure rates

Logs execution times

🔐 Concurrency Control Strategy

The system uses Redis atomic operations to:

Prevent overselling

Guarantee data integrity

Synchronize access to shared resources

Key mechanisms:

Atomic DECR operations

Transaction blocks (MULTI/EXEC)

Centralized in-memory state

Optional locking patterns

This ensures:

Each product unit is sold once

No race conditions occur

High throughput is maintained

📊 Performance Evaluation

Metrics analyzed:

Request throughput

Response time

Success vs failure ratio

System stability under concurrency

Results show:

Stable performance under heavy load

Zero stock inconsistency

Efficient Redis utilization

All charts and figures are available in:

3_Report/figures/


Full technical report:

3_Report/FlashSale_Project_Report.pdf

📈 Project Management & Documentation

Project organization includes:

Gantt chart planning

Jira task tracking

Structured documentation

Annexes and screenshots

Available in:

5_Gantt/
6_Annexes/

🔑 Key Features

Redis-based concurrency management

Client load simulation

Modular architecture

Dockerized environment

Experimental result analysis

Academic-level documentation

🎓 Learning Outcomes

This project enabled the understanding of:

Redis internals and atomicity

Flash sale system design

Concurrent programming

Distributed data consistency

Performance benchmarking

Professional documentation practices

👨‍💻 Author

Khalid Morjane
Anas Lahraoui
University Project – Flash Sale System using Redis

GitHub: https://github.com/KHALIDMRJ
GitHub: https://github.com/anabkl

📜 License

This project is developed strictly for academic and educational purposes.
