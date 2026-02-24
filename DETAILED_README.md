# R2 A3 Holdings - Serverless File Processing Pipeline

**A scalable, event-driven AWS serverless application that downloads holding files from Landmark SFTP servers, parses and processes them, and distributes them to agencies via the CEE integration platform.**

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture at a Glance](#architecture-at-a-glance)
3. [Repository Structure](#repository-structure)
4. [How It Works (Deep Dive)](#how-it-works-deep-dive)
5. [Workflow Diagrams](#workflow-diagrams)
6. [Installation & Setup](#installation--setup)
7. [Configuration](#configuration)
8. [Usage](#usage)
9. [Testing & Quality Assurance](#testing--quality-assurance)
10. [Troubleshooting](#troubleshooting)
11. [Extending the Project](#extending-the-project)
12. [Security & Secrets Management](#security--secrets-management)
13. [Contributing Guidelines](#contributing-guidelines)
14. [License](#license)
15. [Appendix](#appendix)
16. [Open Questions](#open-questions)

---

## Overview

### What This Project Does

The **R2 A3 Holdings** application is a serverless file processing pipeline built on AWS Lambda and Step Functions. It automates the following workflow:

1. **Extract**: Periodically downloads holding files from Landmark SFTP server
2. **Parse**: Validates and parses holding file names to extract agency codes
3. **Process**: Zips holdings files for distribution
4. **Distribute**: Sends processed files to agencies via the CEE (integration platform)
5. **Error Handling**: Routes invalid or failed files to a rejected bucket for audit

### Who It's For

- **Development Teams**: Building integrations with Landmark and agency systems
- **Operations Teams**: Managing file transfer workflows and monitoring holdings distribution
- **New Joiners**: Understanding serverless Python architectures on AWS
- **External Integrators**: Consuming holdings via the CEE platform

### Key Features

- ✅ **Scheduled SFTP Polling** – Runs every 10 minutes via EventBridge scheduler
- ✅ **Event-Driven Processing** – Triggered by S3 object creation
- ✅ **Orchestrated Workflow** – AWS Step Functions for reliable state management
- ✅ **Error Handling** – Automatic retry logic and dead-letter buckets
- ✅ **Multi-Environment Support** – Dev, SIT, UAT, Training, Production environments
- ✅ **Secrets Management** – Secure credential storage via AWS Secrets Manager
- ✅ **Comprehensive Logging** – CloudWatch integration for observability
- ✅ **Infrastructure as Code** – AWS SAM for reproducible deployments

---

## Architecture at a Glance

### Component Overview

| Component | Purpose | Technology |
|-----------|---------|-----------|
| **LandmarkSFTPFileTransferFunction** | Polls Landmark SFTP and copies files to S3 | Lambda + Python 3.13 |
| **SCHEventReceiverFunction** | Receives S3 events and triggers state machine | Lambda + S3 Events |
| **ParseHoldingFileFunction** | Extracts agency code from file name | Lambda + Python |
| **ZipHoldingFileFunction** | Creates ZIP archives of holdings | Lambda + Python + zipfile |
| **GetAgencyIdFromSFFunction** | Queries Salesforce for agency metadata | Lambda + Salesforce API |
| **SendFilesToAgenciesFunction** | Invokes CEE state machine for distribution | Lambda + Step Functions |
| **RejectedFileFunction** | Handles parsing failures | Lambda |
| **HoldingFileStateMachine** | Orchestrates the entire workflow | AWS Step Functions |
| **S3 Buckets** | Data storage for holdings files | AWS S3 |
| **Secrets Manager** | Stores SFTP and Salesforce credentials | AWS Secrets Manager |
| **SSM Parameter Store** | Stores configuration paths and ARNs | AWS Systems Manager |
| **CloudWatch Logs** | Centralized logging and monitoring | AWS CloudWatch |

### High-Level Architecture Diagram

```mermaid
graph TB
    A["EventBridge Scheduler<br/>(Every 10 min)"] -->|Trigger| B["LandmarkSFTPFileTransferFunction"]
    B -->|Write File| C["S3: Holdings Bucket"]
    C -->|S3:ObjectCreated Event| D["SCHEventReceiverFunction"]
    D -->|Start Execution| E["HoldingFileStateMachine<br/>Step Functions"]
    E -->|Parse| F["ParseHoldingFileFunction"]
    F -->|Success Path| G["ZipHoldingFileFunction"]
    F -->|Failure Path| H["S3: Rejected Bucket"]
    G -->|Agency Lookup| I["GetAgencyIdFromSFFunction"]
    I -->|Invoke CEE| J["SendFilesToAgenciesFunction"]
    J -->|Execute CEE StateMachine| K["CEE Platform<br/>External Integration"]
    K -->|Notify| L["Agencies"]
    B -->|Fetch Creds| M["Secrets Manager"]
    B -->|Fetch Config| N["SSM Parameter Store"]
    F -->|Log Events| O["CloudWatch Logs"]
    style E fill:#ff9999
    style C fill:#99ccff
    style H fill:#ffcc99
    style L fill:#99ff99
```

---

## Repository Structure

```
SWM_holdings_a3/
├── README.md                          # Original quick reference
├── DETAILED_README.md                 # This comprehensive guide
├── pyproject.toml                     # Poetry dependencies and project metadata
├── poetry.lock                        # Locked dependency versions
├── bandit.yaml                        # Security scanning configuration
├── GitVersion.yml                     # Semantic versioning rules
├── python-code-check.sh              # Local code quality check script
│
├── r2_int_a3_template.yaml           # AWS SAM Infrastructure as Code (Main)
│   ├─ Parameters: EnvPrefix (r2dev, r2sit, etc.)
│   ├─ Resources: 6 Lambda functions, 1 StateMachine, 3 S3 buckets, 1 LogGroup
│   └─ Outputs: StateMachine ARN and IAM Role ARN
│
├── functions/                         # Lambda function implementations
│   │
│   ├── landmark_sftp/                # SFTP polling function
│   │   ├── a3_landmark_sftp.py       # Entry point: lambda_handler()
│   │   ├── __init__.py
│   │   └── requirements.txt           # Dependency: paramiko
│   │
│   ├── sch_event_receiver/           # S3 event receiver function
│   │   ├── a3_landmark_sch_event_receiver.py  # Entry point: lambda_handler()
│   │   ├── __init__.py
│   │   └── requirements.txt
│   │
│   ├── parse_holding_file/           # File parsing function
│   │   ├── a3_parse_holding_file.py  # Entry point: lambda_handler()
│   │   │                              # Parses: Holdings_YYYY-MM-DD HHMM AGENCYCODE_PROVIDER.h
│   │   ├── __init__.py
│   │   └── requirements.txt
│   │
│   ├── zip_holding_file/             # ZIP creation function
│   │   ├── a3_zip_holding_file.py    # Entry point: lambda_handler()
│   │   ├── __init__.py
│   │   └── requirements.txt
│   │
│   ├── agency_id_sf/                 # Salesforce agency lookup
│   │   ├── a3_get_agency_id_sf.py    # Entry point: lambda_handler()
│   │   │                              # Query: SELECT Id, Name from Account WHERE SWM_External_Holdings_ID__c=
│   │   ├── __init__.py
│   │   └── requirements.txt
│   │
│   ├── send_file_agencies/           # CEE integration function
│   │   ├── a3_send_file_agencies.py  # Entry point: lambda_handler()
│   │   │                              # Constructs CEE message and starts StateMachine
│   │   ├── __init__.py
│   │   └── requirements.txt
│   │
│   └── rejected_file/                # Error handler (placeholder)
│       ├── a3_rejected_file.py       # Entry point: lambda_handler()
│       ├── __init__.py
│       └── requirements.txt
│
├── statemachine/                      # Step Functions definitions
│   └── agent_file_transfer.asl.json  # State machine ASL (Amazon States Language)
│       ├─ States: Parse → Choice → Zip → GetAgency → SendCEE
│       ├─ Retry logic: 3 attempts with 2-second interval
│       └─ Error paths: Failure → RejectedBucket
│
└── pipelines/                         # CI/CD Pipeline
    └── r2_int_a3_pipeline.yaml       # Azure DevOps YAML pipeline
        ├─ Stage 1: code_check         # Black, Bandit, Safety, SAM lint
        ├─ Stage 2: [r2dev|r2sit|r2uat|r2train|prod]_Build  # Build & Deploy
        └─ Stage 3: versionTag         # GitVersion tags
```

### Key File Purposes

| File | Purpose | Key Classes/Functions |
|------|---------|----------------------|
| [r2_int_a3_template.yaml](r2_int_a3_template.yaml) | CloudFormation/SAM template defining all AWS resources | Parameters, Resources (6 Lambdas), Policies, Outputs |
| [functions/landmark_sftp/a3_landmark_sftp.py](functions/landmark_sftp/a3_landmark_sftp.py) | Entry point for SFTP polling | `lambda_handler(event, context)` - invokes Techone SFTP adaptor |
| [functions/sch_event_receiver/a3_landmark_sch_event_receiver.py](functions/sch_event_receiver/a3_landmark_sch_event_receiver.py) | S3 event handler | `lambda_handler(event, context)` - starts state machine |
| [functions/parse_holding_file/a3_parse_holding_file.py](functions/parse_holding_file/a3_parse_holding_file.py) | Validates and parses file names | `lambda_handler(event, context)` - extracts agency code, logs to CEE |
| [functions/zip_holding_file/a3_zip_holding_file.py](functions/zip_holding_file/a3_zip_holding_file.py) | Zips holdings and copies to public bucket | `lambda_handler(event, context)` - uses Python zipfile module |
| [functions/send_file_agencies/a3_send_file_agencies.py](functions/send_file_agencies/a3_send_file_agencies.py) | Invokes CEE platform | `construct_cee_message()`, `invokeCEE()`, `lambda_handler()` |
| [statemachine/agent_file_transfer.asl.json](statemachine/agent_file_transfer.asl.json) | Orchestrates workflow | State definitions, transitions, retry policies |
| [pipelines/r2_int_a3_pipeline.yaml](pipelines/r2_int_a3_pipeline.yaml) | Azure DevOps CI/CD | Lint, Security scan, Build SAM, Package, Deploy, Tag |

---

## How It Works (Deep Dive)

### End-to-End Execution Flow

#### Phase 1: File Ingestion (SFTP → S3)

```
┌─────────────────────────────────────────────────────────────┐
│ EventBridge Scheduler (Every 10 minutes)                    │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│ LandmarkSFTPFileTransferFunction (Lambda)                   │
│ ├─ Retrieves SFTP path from SSM: /${EnvPrefix}/a3/lmk_ftp_path
│ ├─ Retrieves SFTP adaptor ARN: Resolves shared Lambda       │
│ ├─ Constructs request payload:                              │
│ │  ├─ sftp_path: "/ftp.out/DATAEXTRACT/DATA"               │
│ │  ├─ s3_bucket: "seil-{env}-holdings-file"                │
│ │  └─ log_group/log_stream: For audit trail                │
│ ├─ Invokes Techone SFTP Adaptor (shared Lambda)            │
│ │  └─ Adaptor handles SFTP protocol, downloads files       │
│ └─ Files written to S3 with prefix "sftp/..."              │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
        S3 "holdings-file" bucket
        └─ sftp/Holdings_2023-11-29 0209 12345_SEVNET.h
```

**Code Reference**: [a3_landmark_sftp.py](functions/landmark_sftp/a3_landmark_sftp.py#L1-L30)

#### Phase 2: Event Detection & Workflow Trigger

```
┌─────────────────────────────────────────────────────────────┐
│ S3 Event: ObjectCreated                                     │
│ Bucket: seil-{env}-holdings-file                            │
│ Key: sftp/Holdings_2023-11-29 0209 12345_SEVNET.h          │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│ SCHEventReceiverFunction (Lambda)                           │
│ ├─ Receives S3 event from CloudTrail                        │
│ ├─ Extracts latest record (sorted by eventTime)            │
│ ├─ Gets request ID (x-amz-request-id) for tracking         │
│ ├─ Strips "sftp/" prefix from key                          │
│ ├─ Reads file metadata from S3 object metadata             │
│ │  └─ log_group, log_stream (passed by SFTP function)     │
│ ├─ Constructs state machine input:                         │
│ │  {                                                        │
│ │    "filedetails": [{                                      │
│ │      "bucket": "seil-r2dev-holdings-file",              │
│ │      "name": "Holdings_2023-11-29 0209 12345_SEVNET.h", │
│ │      "error_bucket": "seil-r2dev-holdings-file-useraccess",
│ │      "id": "x-amz-request-id"                           │
│ │    }]                                                     │
│ │  }                                                        │
│ └─ Starts HoldingFileStateMachine execution                │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
    Step Functions State Machine Initiated
```

**Code Reference**: [a3_landmark_sch_event_receiver.py](functions/sch_event_receiver/a3_landmark_sch_event_receiver.py#L30-L60)

#### Phase 3: Parsing & Validation

```
┌─────────────────────────────────────────────────────────────┐
│ ParseHoldingFileFunction (Lambda)                           │
│ ├─ Input: Holdings_2023-11-29 0209 12345_SEVNET.h         │
│ ├─ Parsing logic:                                           │
│ │  ├─ Split by space: ["Holdings_2023-11-29", "0209", ...]│
│ │  ├─ Extract part[2]: "12345_SEVNET.h"                   │
│ │  ├─ Split by underscore: ["12345", "SEVNET.h"]          │
│ │  ├─ Agency Code = "12345"                               │
│ └─ Validation:                                              │
│    ├─ If agency_code is empty:                             │
│    │  ├─ Log error to CEE (DEFAULTED status)              │
│    │  └─ Return { status: "failure", bucket: error_bucket} │
│    └─ If agency_code valid:                                │
│       └─ Return { status: "success", file: "...", id, agency_code }
│                                                             │
│ Output: JSON object with parsing result + metadata         │
└──────────────┬──────────────────────────────────────────────┘
               │
        ┌──────┴──────┐
        │ status?     │
        ▼             ▼
    "success"       "failure"
        │             │
        ▼             ▼
   (to Zip)    (to RejectedBucket)
```

**Parsing Algorithm**: 
- File name format: `Holdings_YYYY-MM-DD HHMM AGENCYCODE_PROVIDER.h`
- Splits on space, extracts index [2] = `AGENCYCODE_PROVIDER.h`
- Re-splits on underscores, [0] = Agency code

**Code Reference**: [a3_parse_holding_file.py](functions/parse_holding_file/a3_parse_holding_file.py#L51-L100)

#### Phase 4: Success Path - ZIP & Send

If parsing succeeds:

```
┌─────────────────────────────────────────────────────────────┐
│ ZipHoldingFileFunction (Lambda)                             │
│ ├─ Receives: id, bucket, file, agency_code                │
│ ├─ Fetches file from S3 bucket                             │
│ ├─ Creates in-memory ZIP file via Python zipfile module    │
│ ├─ Uploads to: seil-{env}-holdings-file-zip/{file}.zip    │
│ ├─ Copies original to user access bucket:                  │
│ │  └─ seil-{env}-holdings-file-useraccess/{file}          │
│ └─ Output: { id, file, bucket, file_zip_name, ... }      │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│ GetAgencyIdFromSFFunction (Lambda)                          │
│ ├─ Input: agency_code = "12345"                            │
│ ├─ Queries Salesforce via SF Adaptor:                      │
│ │  SELECT Id, Name from Account                            │
│ │  WHERE SWM_External_Holdings_ID__c = "12345"            │
│ ├─ Returns: Salesforce Account ID and Name                 │
│ └─ Used for CEE integration context                        │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│ SendFilesToAgenciesFunction (Lambda)                        │
│ ├─ Constructs CEE message:                                  │
│ │  {                                                        │
│ │    "id": "request-id",                                   │
│ │    "body": {                                              │
│ │      "externalHoldingsId": "12345",                      │
│ │      "type": "Holdings",                                 │
│ │      "files": [{                                          │
│ │        "path": "s3://seil-r2dev-holdings-file-zip/..."  │
│ │      }]                                                   │
│ │    }                                                      │
│ │  }                                                        │
│ ├─ Invokes CEE StateMachine via Step Functions             │
│ ├─ Response: CEE execution ARN and status                  │
│ └─ Workflow complete                                        │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
        External CEE Platform
        (Handles Agency Distribution)
```

**Code References**:
- [a3_zip_holding_file.py](functions/zip_holding_file/a3_zip_holding_file.py#L17-L40)
- [a3_send_file_agencies.py](functions/send_file_agencies/a3_send_file_agencies.py#L10-L45)

#### Phase 5: Failure Path - Error Handling

If parsing fails:

```
┌──────────────────────────────────────────────┐
│ ParseHoldingFileFunction returns:             │
│ { status: "failure", bucket, copySource, ... }
└──────────────┬───────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────┐
│ State Machine Choice: Is File Parsing        │
│ Successful?                                   │
│ ├─ Choice rule: $.status == "failure"       │
│ └─ Transition: "Send file to Rejected Bucket"│
└──────────────┬───────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────┐
│ S3 CopyObject (State Machine Task)           │
│ ├─ Source: Original holding file (holdings  │
│ │           bucket)                          │
│ ├─ Dest: Rejected/error bucket for audit    │
│ └─ File moved for investigation              │
└──────────────────────────────────────────────┘
```

**Code Reference**: [agent_file_transfer.asl.json](statemachine/agent_file_transfer.asl.json#L30-L40)

### Module-Level Architecture

#### Core Functions

| Function | Responsibility | Timeout | Memory |
|----------|----------------|---------|--------|
| **LandmarkSFTPFileTransferFunction** | SFTP polling, invokes adaptor | 900s (15 min) | 512 MB |
| **SCHEventReceiverFunction** | S3 event parsing, workflow trigger | 30s (default) | 128 MB |
| **ParseHoldingFileFunction** | File name parsing, validation | 30s (default) | 128 MB |
| **ZipHoldingFileFunction** | ZIP creation, file distribution | 30s (default) | 512 MB |
| **GetAgencyIdFromSFFunction** | Salesforce integration | 30s (default) | 128 MB |
| **SendFilesToAgenciesFunction** | CEE platform invocation | 30s (default) | 128 MB |
| **RejectedFileFunction** | Error handling stub | 30s (default) | 128 MB |

#### State Machine Flow

```json
┌─ Parse Holding File
│  ├─ Retry: 3 attempts, 2-second intervals
│  └─ Next: Is File Parsing Successful?
│
├─ Is File Parsing Successful? (Choice)
│  ├─ If $.status == "success" ──► Zip Holding file
│  └─ If $.status == "failure"  ──► Send to Rejected Bucket ──► END
│
├─ Zip Holding file
│  ├─ Retry: 3 attempts, 2-second intervals
│  └─ Next: Send Files to Agencies via CEE
│
└─ Send Files to Agencies via CEE
   ├─ Retry: 3 attempts, 2-second intervals
   └─ END
```

**Code Reference**: [agent_file_transfer.asl.json](statemachine/agent_file_transfer.asl.json)

---

## Workflow Diagrams

### Diagram 1: High-Level System Flow

```mermaid
graph TD
    A["EventBridge Scheduler<br/>(Every 10 minutes)<br/>Launches SFTP polling"]
    B["LandmarkSFTPFileTransferFunction<br/>Fetch from SFTP, Write to S3"]
    C["S3 Holdings Bucket<br/>seil-{env}-holdings-file"]
    D["S3 Event Notification<br/>s3:ObjectCreated"]
    E["SCHEventReceiverFunction<br/>Parse Event, Extract File Details"]
    F["HoldingFileStateMachine<br/>Orchestrator"]
    G["ParseHoldingFileFunction<br/>Extract Agency Code"]
    H{"Parsing<br/>Successful?"}
    I["ZipHoldingFileFunction<br/>Create ZIP Archive"]
    J["GetAgencyIdFromSFFunction<br/>Query Salesforce"]
    K["SendFilesToAgenciesFunction<br/>Invoke CEE"]
    L["CEE Platform<br/>Final Destination"]
    M["S3 Rejected Bucket<br/>seil-{env}-holdings-file-useraccess"]
    N["Secrets Manager<br/>SFTP & SF Credentials"]
    O["SSM Parameter Store<br/>Config & ARNs"]
    
    A --> B
    B --> N
    B --> O
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    H -->|Success| I
    H -->|Failure| M
    I --> J
    J --> K
    K --> L
    
    style F fill:#ff6b6b
    style C fill:#4ecdc4
    style M fill:#ffe66d
    style L fill:#95e1d3
    style A fill:#c7ceea
```

### Diagram 2: State Machine Detail with Retry Logic

```mermaid
stateDiagram-v2
    [*] --> ParseHoldingFile
    
    ParseHoldingFile --> IsParsingSuccessful: On Complete
    note right of ParseHoldingFile
        Retry: 3 attempts
        Interval: 2 seconds
        Backoff: 1.0x
    end note
    
    IsParsingSuccessful --> ZipHoldingFile: status=success
    IsParsingSuccessful --> SendRejected: status=failure
    
    ZipHoldingFile --> SendFilesToAgencies: On Complete
    note right of ZipHoldingFile
        Retry: 3 attempts
        Interval: 2 seconds
        Backoff: 1.0x
    end note
    
    SendFilesToAgencies --> [*]: On Complete
    note right of SendFilesToAgencies
        Retry: 3 attempts
        Interval: 2 seconds
        Backoff: 1.0x
    end note
    
    SendRejected --> [*]: Copy to Error Bucket
    
    style ParseHoldingFile fill:#e1bee7
    style IsParsingSuccessful fill:#fff9c4
    style ZipHoldingFile fill:#c5e1a5
    style SendFilesToAgencies fill:#b2dfdb
    style SendRejected fill:#ffccbc
```

### Diagram 3: Request-to-Response Sequence

```mermaid
sequenceDiagram
    participant EB as EventBridge
    participant SFTP as SFTP Function
    participant S3 as S3 Bucket
    participant SER as Event Receiver
    participant SM as State Machine
    participant Parse as Parse Function
    participant Zip as Zip Function
    participant SF as Salesforce
    participant Send as Send Function
    participant CEE as CEE Platform
    
    EB->>SFTP: Trigger (every 10 min)
    SFTP->>SFTP: Fetch from Landmark SFTP
    SFTP->>S3: Upload file sftp/Holdings_...h
    S3->>SER: S3 Event (ObjectCreated)
    SER->>SM: StartExecution(fileDetails)
    
    SM->>Parse: Invoke
    Parse->>Parse: Extract agency code from filename
    alt Parsing Successful
        Parse-->>SM: {status: success, agency_code}
        SM->>Zip: Invoke
        Zip->>S3: Download original file
        Zip->>Zip: Create ZIP in-memory
        Zip->>S3: Upload ZIP to holdings-file-zip bucket
        Zip->>S3: Copy original to useraccess bucket
        Zip-->>SM: {success}
        
        SM->>SF: Invoke GetAgencyId
        SF->>SF: Query Account WHERE id=12345
        SF-->>SM: {accountId, ...}
        
        SM->>Send: Invoke
        Send->>Send: Construct CEE message
        Send->>CEE: StartExecution(CEE StateMachine)
        CEE-->>Send: {executionArn, ...}
        Send-->>SM: {success}
        SM-->>EB: Execution Complete
    else Parsing Failed
        Parse-->>SM: {status: failure}
        SM->>S3: CopyObject to rejected bucket
        SM-->>EB: Execution Complete (with error path)
    end
```

### Diagram 4: Data Flow - File Lifecycle

```mermaid
graph LR
    FTP["Landmark<br/>SFTP Server<br/>Holdings_*_*.h"] 
    B1["S3 Holdings Bucket<br/>(seil-{env}<br/>-holdings-file)"]
    B2["S3 ZIP Bucket<br/>(seil-{env}<br/>-holdings-file-zip)"]
    B3["S3 User Access<br/>(seil-{env}<br/>-holdings-file-useraccess)"]
    B4["S3 Rejected<br/>(on error)"]
    CEE["CEE Platform<br/>External System"]
    AGE["Agencies"]
    
    FTP -->|SFTP Adaptor| B1
    B1 -->|Parse| B1
    B1 -->|Success: Zip| B2
    B1 -->|Copy Original| B3
    B1 -->|Failure: Copy| B4
    B2 -->|S3 Path in<br/>CEE Message| CEE
    CEE -->|Distribute| AGE
    B3 -->|Available for<br/>Admin Review| AGE
    B4 -->|Manual Review<br/>Required| AGE
    
    style FTP fill:#90caf9
    style B1 fill:#80deea
    style B2 fill:#80cbc4
    style B3 fill:#c8e6c9
    style B4 fill:#ffab91
    style CEE fill:#ffe082
    style AGE fill:#a5d6a7
```

---

## Installation & Setup

### Prerequisites

- **AWS Account** with appropriate permissions (IAM, Lambda, S3, Secrets Manager, SSM, Step Functions, CloudWatch)
- **Python 3.13** (for local development)
- **Poetry** (Python dependency manager)
- **AWS CLI** (for deployment)
- **AWS SAM CLI** (for local testing and deployment)
- **Git** (for version control)
- **Azure DevOps** (for CI/CD pipeline, if using)
- **Landmark SFTP Account** with credentials
- **Salesforce Account** with read access to Account objects

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd SWM_holdings_a3
```

### Step 2: Install Python Dependencies

#### Using Poetry (Recommended)

```bash
# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Install project dependencies
poetry install
```

#### Using pip (Alternative)

```bash
# Create virtual environment
python3.13 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install from requirements
pip install poetry
poetry install --no-root
```

### Step 3: Install AWS SAM CLI

```bash
# macOS (using Homebrew)
brew tap aws/tap
brew install aws-sam-cli

# Linux or other platforms: See https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html
```

### Step 4: Configure AWS Credentials

```bash
aws configure

# Enter:
# - AWS Access Key ID
# - AWS Secret Access Key
# - Default region: ap-southeast-2
# - Default output format: json
```

### Step 5: Setup Secrets & Parameters

Before deployment, ensure the following secrets and parameters exist in AWS Systems Manager:

#### AWS Secrets Manager

```bash
# SFTP Credentials
aws secretsmanager create-secret \
  --name /r2dev/lmk_ftp \
  --secret-string '{
    "ftp_url": "sftp.landmark.example.com",
    "user_id": "username",
    "password": "password",
    "key_value": "ssh_key_content"
  }'

# Salesforce API Credentials
aws secretsmanager create-secret \
  --name /r2dev/salesforce/service_account \
  --secret-string '{
    "client_id": "your_client_id",
    "client_secret": "your_secret",
    "username": "api_user@org.salesforce.com",
    "password": "password+token"
  }'
```

#### AWS Systems Manager Parameter Store

```bash
# SFTP Path
aws ssm put-parameter \
  --name /r2dev/a3/lmk_ftppath \
  --value "/ftp.out/DATAEXTRACT/DATA" \
  --type String
  --region ap-southeast-2

# Secret Manager Reference
aws ssm put-parameter \
  --name /r2dev/a3/lmk_secretname \
  --value "r2dev/lmk_ftp" \
  --type String
  --region ap-southeast-2

# Lambda ARNs (shared functions from common layer)
aws ssm put-parameter \
  --name /r2dev/lambda-arn/common \
  --value "arn:aws:lambda:ap-southeast-2:123456789:function:lmk-sftp-adaptor" \
  --type String
  --region ap-southeast-2

# CEE State Machine ARN
aws ssm put-parameter \
  --name /r2dev/cee/requestReceiverARN \
  --value "arn:aws:states:ap-southeast-2:123456789:stateMachine:cee-receiver" \
  --type String
  --region ap-southeast-2

# Salesforce Adaptor
aws ssm put-parameter \
  --name /r2dev/lambda-arn/common/sf-adaptor \
  --value "arn:aws:lambda:ap-southeast-2:123456789:function:sf-query-adaptor" \
  --type String
  --region ap-southeast-2
```

---

## Configuration

### Environment Variables

All configuration is managed through AWS Systems Manager Parameter Store and Secrets Manager. The SAM template substitutes environment variables at deployment time.

#### Template Parameters (in r2_int_a3_template.yaml)

| Parameter | Purpose | Example | Notes |
|-----------|---------|---------|-------|
| `EnvPrefix` | Environment identifier | `r2dev`, `r2sit`, `r2uat`, `r2train`, `prod` | Substituted in all resource names |

#### Lambda Environment Variables

**LandmarkSFTPFileTransferFunction**:
```yaml
LANDMARK_SFTP_PATH: /${EnvPrefix}/a3/lmk_ftppath
SFTP_S3_BUCKET_PA: seil-${EnvPrefix}-holdings-file
SFTP_S3_BUCKET_AUA: seil-${EnvPrefix}-holdings-file-useraccess
LOG_GROUP_NAME: /aws/seil/${EnvPrefix}/a3
LMK_SFTP_ADAPTOR_FUNCTION: /${EnvPrefix}/lambda-arn/common/lmk-sftp-adaptor
```

**SCHEventReceiverFunction**:
```yaml
Program_Schedule_StateMachine: HoldingFileStateMachine ARN
Error_Bucket: seil-${EnvPrefix}-holdings-file-useraccess
```

**ParseHoldingFileFunction**:
```yaml
CEE_LOG_GROUP_NAME: /aws/lambda/${EnvPrefix}/CEESESNotificationLogHandler
CEE_LOG_STREAM_NAME: ${EnvPrefix}_cee_notification_stream
```

**ZipHoldingFileFunction**:
```yaml
HOLDING_ZIP_BUCKET: seil-${EnvPrefix}-holdings-file-zip
USER_BUCKET: seil-${EnvPrefix}-holdings-file-useraccess
```

**Send FilesToAgenciesFunction**:
```yaml
SFTP_S3_BUCKET: seil-${EnvPrefix}-sch-file-in
CEE_FUNCTION: /${EnvPrefix}/cee/requestReceiverARN
```

**GetAgencyIdFromSFFunction**:
```yaml
SF_ADAPTOR: /${EnvPrefix}/lambda-arn/common/sf-adaptor
SFTP_S3_BUCKET: seil-${EnvPrefix}-sch-file-in
```

### S3 Buckets

Three S3 buckets are created per environment:

| Bucket Name | Purpose | Lifecycle |
|------------|---------|-----------|
| `seil-{env}-holdings-file` | Primary holdings file storage (private) | Retain |
| `seil-{env}-holdings-file-zip` | ZIP archive storage for distribution | Expire after 30 days |
| `seil-{env}-holdings-file-useraccess` | Public/user access copies + rejected files | Retain with versioning |

### Lambda IAM Permissions

Each Lambda function has scoped IAM policies defined in the SAM template. Key permissions:

- **S3 Access**: Get, Put, Delete objects, List buckets
- **Secrets Manager**: Read secret values for credentials
- **SSM Parameter Store**: Read parameters for config
- **Step Functions**: Invoke state machines
- **CloudWatch Logs**: Create and write logs
- **Lambda**: Invoke other functions (adaptor lambdas)

### EventBridge Schedule

The SFTP polling is triggered by EventBridge scheduler with:
- **Schedule Expression**: `rate(10 minutes)`
- **Timezone**: `Australia/Sydney`
- **Function**: `LandmarkSFTPFileTransferFunction`

---

## Usage

### Deployment

#### Deploy to AWS (Using SAM)

```bash
# Build and validate locally
sam validate --lint --template r2_int_a3_template.yaml

# Build the project
sam build --template-file r2_int_a3_template.yaml

# Deploy to AWS (guided mode - first time)
sam deploy --guided \
  --stack-name seil-r2dev-a3 \
  --parameter-overrides EnvPrefix=r2dev \
  --region ap-southeast-2

# Deploy subsequent times (non-interactive)
sam deploy \
  --template-file .aws-sam/build/template.yaml \
  --stack-name seil-r2dev-a3 \
  --parameter-overrides EnvPrefix=r2dev \
  --capabilities CAPABILITY_IAM \
  --region ap-southeast-2
```

#### Deploy via CI/CD Pipeline

```bash
# Push to repository with tag
git commit -am "Feature: Add new parsing logic"
git push origin main

# Pipeline automatically:
# 1. Runs code quality checks (Black, Bandit, Safety)
# 2. Validates SAM template
# 3. Builds Lambda layers and functions
# 4. Packages with SAM
# 5. Deploys to chosen environment
# 6. Tags with semantic version
```

### Manual Testing

#### Test SFTP Function Locally

```bash
# Create test event
cat > test-sftp.json << 'EOF'
{
  "id": "test-event-12345"
}
EOF

# Invoke function locally
sam local invoke LandmarkSFTPFileTransferFunction --event test-sftp.json
```

#### Test State Machine Locally

```bash
# Start local DynamoDB (if needed)
sam local start-dynamodb

# Start local Lambda container
sam local start-api

# Invoke state machine
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:local:123456789:stateMachine:r2dev-lmk-to-agent-holding-file \
  --input '{"filedetails": [{"bucket": "test-bucket", "name": "Holdings_2023-11-29 0209 12345_TEST.h", "error_bucket": "error-bucket", "id": "req-123"}]}' \
  --endpoint-url http://localhost:8080
```

#### Test Parsing Function

```bash
# Create test file name
TEST_FILE="Holdings_2023-11-29 0209 12345_SEVNET.h"

# Create test event
cat > test-parse.json << EOF
{
  "filedetails": [{
    "bucket": "seil-r2dev-holdings-file",
    "name": "$TEST_FILE",
    "error_bucket": "seil-r2dev-holdings-file-useraccess",
    "id": "test-req-id"
  }]
}
EOF

# Invoke parse function
sam local invoke ParseHoldingFileFunction --event test-parse.json
```

#### Test ZIP Function

```bash
# Create test event
cat > test-zip.json << 'EOF'
{
  "id": "test-id",
  "bucket": "seil-r2dev-holdings-file",
  "file": "Holdings_2023-11-29 0209 12345_SEVNET.h"
}
EOF

# Note: This requires S3 bucket to actually exist and contain the file
sam local invoke ZipHoldingFileFunction --event test-zip.json
```

### Example: Full Workflow Test

```bash
# 1. Upload test file to S3
aws s3 cp Holdings_2023-11-29.h \
  s3://seil-r2dev-holdings-file/sftp/Holdings_2023-11-29\ 0209\ 12345_SEVNET.h

# 2. Verify event was triggered (check CloudWatch logs)
aws logs tail /aws/lambda/r2dev-sch-event-receiver-function --follow

# 3. Check state machine execution
aws stepfunctions list-executions \
  --state-machine-arn $(aws stepfunctions describe-state-machine \
    --name r2dev-lmk-to-agent-holding-file \
    --query 'stateMachineArn' \
    --output text) \
  --region ap-southeast-2

# 4. Get execution details
aws stepfunctions describe-execution \
  --execution-arn arn:aws:states:ap-southeast-2:123456789:execution:r2dev... \
  --region ap-southeast-2

# 5. Verify ZIP was created
aws s3 ls s3://seil-r2dev-holdings-file-zip/ --region ap-southeast-2
```

---

## Testing & Quality Assurance

### Local Code Quality Checks

```bash
# Run all checks
./python-code-check.sh bandit.yaml functions $SAFETY_API_KEY

# Individual checks:

# Format code with Black
python -m black .

# Security scanning with Bandit
bandit -c bandit.yaml -r functions/

# Dependency vulnerability check with Safety
pip install safety
safety check --key=$SAFETY_API_KEY -i 52495 -i 60841

# SAM template validation
sam validate --lint --template r2_int_a3_template.yaml
```

### Unit Tests (Future)

The pipeline includes commented-out unit test stage. To enable:

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run tests
pytest -s --show-capture=stdout -vv --cov=functions

# Generate coverage report
coverage html
open htmlcov/index.html
```

### Integration Tests

Run against AWS resources:

```bash
# Deploy to test environment
sam deploy \
  --stack-name seil-r2dev-a3-test \
  --capabilities CAPABILITY_IAM

# Run integration tests
pytest tests/integration/ -v --region ap-southeast-2
```

### Code Quality Standards

- **Formatting**: Black (line length: 88 chars)
- **Security**: Bandit (B311, B608 skipped)
- **Dependencies**: Safety vulnerability scanning
- **Linting**: SAM template validation
- **Python Version**: 3.13 (no legacy Python 2 support)

### CI/CD Pipeline Validation

The Azure DevOps pipeline enforces:

1. **Code Quality** - Black format check, Bandit security scan, Safety dependencies
2. **SAM Validation** - CloudFormation template syntax and best practices
3. **Build** - SAM build for each function
4. **Package** - Uploads to S3 with --force-upload
5. **Deploy** - Creates/updates CloudFormation stack
6. **Version Tagging** - Semantic versioning with GitVersion

---

## Troubleshooting

### Common Issues

#### Issue: Lambda Function Timeout

**Symptom**: `Task timed out after 30.0 seconds`

**Root Cause**: 
- Default Lambda timeout is 30 seconds, but operations may take longer
- SFTP connection timeout
- Large file processing

**Solution**:
```bash
# Update timeout in r2_int_a3_template.yaml
LandmarkSFTPFileTransferFunction:
  Timeout: 900  # or appropriate duration in seconds

# Re-deploy
sam deploy --capabilities CAPABILITY_IAM
```

#### Issue: S3 File Not Found

**Symptom**: `NoSuchKey: The specified key does not exist`

**Root Cause**: 
- File path has unusual characters or spaces
- File wasn't uploaded to correct bucket
- File name mismatch

**Solution**:
```bash
# Check uploaded files
aws s3 ls s3://seil-r2dev-holdings-file/sftp/ --recursive

# Check for special characters
aws s3api list-objects-v2 \
  --bucket seil-r2dev-holdings-file \
  --prefix sftp/ \
  --query 'Contents[*].Key'

# If file has spaces, may need URL encoding
# Replace spaces with %20 in S3 key
```

#### Issue: Parsing Failure - Agency Code Not Extracted

**Symptom**: File moved to rejected bucket, CEE logs show "SFERRORDEFAULTED"

**Root Cause**: 
- File name doesn't match expected format
- Expected format: `Holdings_YYYY-MM-DD HHMM AGENCYCODE_PROVIDER.h`

**Solution**:
```bash
# Check file name format
LOG_GROUP="/aws/seil/r2dev/a3"
LOG_STREAM="your-stream-id"

aws logs get-log-events \
  --log-group-name $LOG_GROUP \
  --log-stream-name $LOG_STREAM \
  --query 'events[?contains(message, `fileNameSplit`)]'

# Expected parsing:
# Input: "Holdings_2023-11-29 0209 12345_SEVNET.h"
# After split: ["Holdings_2023-11-29", "0209", "12345_SEVNET.h"]
# Extract [2]: "12345_SEVNET.h"
# Agency code: "12345"
```

#### Issue: Salesforce Integration Fails

**Symptom**: `{"error": "Invalid credentials"}` in logs

**Root Cause**: 
- Salesforce credentials expired or incorrect
- Org doesn't have External Holdings ID field
- API user doesn't have Account read permission

**Solution**:
```bash
# Verify Salesforce credentials in Secrets Manager
aws secretsmanager get-secret-value \
  --secret-id /r2dev/salesforce/service_account

# Test query in Salesforce Developer Console
SELECT Id, Name from Account WHERE SWM_External_Holdings_ID__c = '12345' LIMIT 1

# Ensure API user has appropriate permission sets assigned
```

#### Issue: CEE StateMachine Invocation Fails

**Symptom**: `AccessDenied: User is not authorized to perform: states:StartExecution`

**Root Cause**: 
- Lambda execution role missing IAM policy for Step Functions
- CEE StateMachine ARN incorrect in Parameter Store

**Solution**:
```bash
# Verify IAM role has states:StartExecution permission
aws iam get-role \
  --role-name seil-r2dev-a3-SendFilesToAgenciesFunctionRole

# Check CEE StateMachine ARN
aws ssm get-parameter \
  --name /r2dev/cee/requestReceiverARN \
  --region ap-southeast-2

# Verify state machine exists
aws stepfunctions describe-state-machine \
  --state-machine-arn <ARN from Parameter Store>
```

### Debugging Tips

#### Enable Debug Logging

```python
# In lambda_handler():
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

logger.debug(f"Event received: {json.dumps(event)}")
logger.debug(f"Environment: {os.environ}")
```

#### Monitor CloudWatch Logs

```bash
# Tail logs in real-time
aws logs tail /aws/seil/r2dev/a3 --follow

# Search for errors
aws logs filter-log-events \
  --log-group-name /aws/seil/r2dev/a3 \
  --filter-pattern "ERROR"

# Get metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Duration \
  --dimensions Name=FunctionName,Value=LandmarkSFTPFileTransferFunction \
  --statistics Average,Maximum \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-02T00:00:00Z \
  --period 3600
```

#### Check State Machine Execution History

```bash
# List recent executions
aws stepfunctions list-executions \
  --state-machine-arn <STATE_MACHINE_ARN> \
  --status-filter FAILED

# Get execution details
aws stepfunctions describe-execution \
  --execution-arn <EXECUTION_ARN>

# Get full execution history
aws stepfunctions get-execution-history \
  --execution-arn <EXECUTION_ARN> \
  --query 'events[*].[id,type,timestamp,executionFailedEventDetails]'
```

#### Test with Sample Data

```bash
# Create realistic test file
cat > /tmp/Holdings_2024-01-15\ 0930\ 99999_TEST.h << 'EOF'
[File content headers...]
Data rows...
EOF

# Upload
aws s3 cp "/tmp/Holdings_2024-01-15 0930 99999_TEST.h" \
  s3://seil-r2dev-holdings-file/sftp/

# Monitor execution
aws logs tail /aws/seil/r2dev/a3 --follow &
# Check S3 buckets
watch 'aws s3 ls s3://seil-r2dev-holdings-file-zip/ && \
       aws s3 ls s3://seil-r2dev-holdings-file-useraccess/'
```

---

## Extending the Project

### How to Add a New Function

#### Step 1: Create Function Directory

```bash
mkdir -p functions/new_feature/{__init__.py,requirements.txt,a3_new_feature.py}
```

#### Step 2: Implement Lambda Handler

```python
# functions/new_feature/a3_new_feature.py
import logging
import json
import boto3

logger = logging.getLogger("a3_new_feature")
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    New feature description.
    
    Args:
        event: Event payload from previous step
        context: Lambda context object
        
    Returns:
        dict: Output for next step in state machine
    """
    logger.info(f"Starting new_feature with event: {event}")
    
    try:
        # Process event
        result = {
            "status": "success",
            "data": processed_data
        }
        logger.info(f"new_feature completed: {result}")
        return result
    except Exception as e:
        logger.exception(f"Error in new_feature: {e}")
        return {
            "status": "failure",
            "error": str(e)
        }
```

#### Step 3: Add Requirements

```bash
# functions/new_feature/requirements.txt
boto3>=1.39.0
requests>=2.31.1
```

#### Step 4: Update SAM Template

Add to `r2_int_a3_template.yaml`:

```yaml
NewFeatureFunction:
  Type: AWS::Serverless::Function
  Properties:
    CodeUri: functions/new_feature/
    Handler: a3_new_feature.lambda_handler
    Runtime: python3.13
    Architectures:
      - x86_64
    Environment:
      Variables:
        NEW_FEATURE_CONFIG: !Sub "/${EnvPrefix}/new_feature/config"
    Policies:
      - Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Action:
              - ssm:GetParameter
              - s3:GetObject
              - s3:PutObject
            Resource: "*"

# Add to HoldingFileStateMachine Policies:
- LambdaInvokePolicy:
    FunctionName: !Ref NewFeatureFunction
```

#### Step 5: Integrate into State Machine

Update `statemachine/agent_file_transfer.asl.json`:

```json
{
  "Comment": "Updated state machine with new feature",
  "StartAt": "Parse Holding File",
  "States": {
    "New Feature": {
      "Type": "Task",
      "Resource": "${NewFeatureFunctionArn}",
      "Next": "Next State",
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "IntervalSeconds": 2,
          "MaxAttempts": 3,
          "BackoffRate": 1
        }
      ]
    }
  }
}
```

#### Step 6: Add to Pipeline

In `pipelines/r2_int_a3_pipeline.yaml`, the pipeline will automatically pick up the new function.

### How to Modify Parsing Logic

File name parsing happens in [a3_parse_holding_file.py](functions/parse_holding_file/a3_parse_holding_file.py):

**Current Format**:
```
Holdings_YYYY-MM-DD HHMM AGENCYCODE_PROVIDER.h
         ^^^^^^^^^^ ^^^^ ^^^^^^^^^^ ^^^^^^^^
         Date     Time   Agency     Provider
```

**To change parsing**:

1. Update the split logic:
```python
def lambda_handler(event, context):
    fileName = fileDetails[0]["name"]
    
    # OLD: fileNameSplit = fileName.split(" "); agencyCode = fileNameSplit[2].split("_")[0]
    # NEW: Custom parsing logic here
    
    # Example: If format changes to Holdings_AGENCYCODE-YYYYMMDD.h
    # agencyCode = fileName.split("_")[1].split("-")[0]
```

2. Update tests:
```python
# tests/test_parse_holding_file.py
def test_new_format():
    event = {"filedetails": [{"name": "Holdings_99999-20240115.h", ...}]}
    result = parse_holding_file.lambda_handler(event, None)
    assert result["agency_code"] == "99999"
```

### How to Add a New Environment

1. **Update Parameter Store** (in AWS):
```bash
# For new environment r2qa
aws ssm put-parameter --name /r2qa/a3/lmk_ftppath --value "..." --region ap-southeast-2
aws ssm put-parameter --name /r2qa/a3/lmk_secretname --value "r2qa/lmk_ftp" --region ap-southeast-2
# ... other parameters
```

2. **Update Pipeline** (in `pipelines/r2_int_a3_pipeline.yaml`):
```yaml
parameters:
  - name: env
    displayName: Environment
    type: string
    default: "r2dev"
    values:
      - r2dev
      - r2sit
      - r2qa        # Add new environment
      - r2uat
      - r2train
      - prod
```

3. **Deploy**:
```bash
# Portal will now allow selection of r2qa
# OR via CLI:
sam deploy \
  --stack-name seil-r2qa-a3 \
  --parameter-overrides EnvPrefix=r2qa \
  --capabilities CAPABILITY_IAM
```

### Common Modification Points

| Modification | File | Change |
|--------------|------|--------|
| Change SFTP polling frequency | `r2_int_a3_template.yaml` line ~55 | `ScheduleExpression: "rate(X minutes)"` |
| Add S3 bucket retention | `r2_int_a3_template.yaml` | Add `LifecycleConfig` to bucket resource |
| Modify file naming convention | `functions/parse_holding_file/a3_parse_holding_file.py` | Update split/parsing logic |
| Add new validation rules | `functions/parse_holding_file/a3_parse_holding_file.py` | Add validation in handler |
| Change ZIP compression | `functions/zip_holding_file/a3_zip_holding_file.py` line ~37 | Change `zipfile.ZIP_DEFLATED` |
| Update CEE message structure | `functions/send_file_agencies/a3_send_file_agencies.py` line ~12 | Modify `construct_cee_message()` |
| Add new step to workflow | `statemachine/agent_file_transfer.asl.json` | Add Task state and transitions |
| Add Lambda permissions | `r2_int_a3_template.yaml` | Add `Policies` section to function |

---

## Security & Secrets Management

### Credentials Storage

All sensitive credentials are stored in AWS Secrets Manager and never committed to code:

#### SFTP Credentials
```json
{
  "ftp_url": "sftp.landmark.example.com",
  "user_id": "username",
  "password": "secure_password",
  "key_value": "-----BEGIN RSA PRIVATE KEY-----\n..."
}
```

Accessed by: `LandmarkSFTPFileTransferFunction` → Techone SFTP Adaptor (shared Lambda)

#### Salesforce Service Account
```json
{
  "client_id": "3MVG9...",
  "client_secret": "secure_secret",
  "username": "api_user@org.salesforce.com",
  "password": "password+security_token"
}
```

Accessed by: `GetAgencyIdFromSFFunction` → SF Adaptor (shared Lambda)

### IAM Policies

Each Lambda has **least privilege** permissions:

```yaml
LandmarkSFTPFileTransferFunction:
  - Secrets Manager: Read only (specific secret)
  - SSM Parameter Store: Read only (specific parameters)
  - S3: Put object only (specific buckets)
  - Lambda: Invoke only (specific adaptor functions)

SCHEventReceiverFunction:
  - S3: Get/List/Put object (specific buckets)
  - Step Functions: Start execution (specific state machine)

ParseHoldingFileFunction:
  - CloudWatch Logs: Create/Write (specific log group)

# etc...
```

### Encryption

- **Secrets Manager**: Encrypted with AWS KMS (customer managed or AWS managed key)
- **S3 Buckets**: Server-side encryption with S3-managed keys (default) or KMS
- **Lambda Environment Variables**: Encrypted at rest by AWS Lambda service
- **CloudWatch Logs**: Encrypted at rest using CloudWatch Logs KMS integration

### Network Security

- **Lambda Execution**: Runs in AWS VPC (if configured via SAM)
- **S3 Access**: Via IAM policies (no public access enabled)
- **SFTP Connection**: Private endpoint or IP whitelisting (configured outside this project)

### Compliance Notes

- **Data Retention**: CloudWatch logs retained 30 days, S3 objects per bucket lifecycle
- **Audit Trail**: All S3 operations logged to CloudTrail
- **Access Logging**: IAM service control policies (SCPs) can enforce additional restrictions
- **Encryption Keys**: KMS key policies reviewed by Security team

---

## Contributing Guidelines

### Development Workflow

1. **Create a Feature Branch**:
```bash
git checkout -b feature/my-feature
# or
git checkout -b fix/my-bugfix
```

2. **Implement Changes**:
   - Follow existing code style (Black formatted)
   - Add docstrings to functions
   - Include error handling and logging

3. **Run Local Quality Checks**:
```bash
./python-code-check.sh bandit.yaml functions $SAFETY_API_KEY
```

4. **Test Locally**:
```bash
sam build
sam local invoke MyFunction --event test-event.json
```

5. **Commit with Conventional Commits**:
```bash
git commit -m "feat: add new validation for agency codes"
# or
git commit -m "fix: correct timezone handling in date parsing"
```

### Pull Request Process

1. Push to feature branch
2. Create Pull Request with description of changes
3. Pipeline automatically runs code quality checks
4. Request review from team members
5. Address review comments
6. Merge to main branch
7. Pipeline automatically builds, tests, and creates semantic version tag

### Coding Standards

- **Python Version**: 3.13 (type hints expected)
- **Formatting**: Black (auto-format)
- **Imports**: `from __future__ import annotations` for type hints
- **Logging**: Use `logging` module, not `print()`
- **Error Handling**: Specific exception types, informative messages
- **Documentation**: Docstrings for all public functions

Example:

```python
"""Module for parsing holding file metadata."""

from __future__ import annotations
import logging
from typing import dict, Optional

logger = logging.getLogger(__name__)

def extract_agency_code(filename: str) -> Optional[str]:
    """
    Extract agency code from holding file name.
    
    Expected format: Holdings_YYYY-MM-DD HHMM AGENCYCODE_PROVIDER.h
    
    Args:
        filename: Full file name including extension
        
    Returns:
        Agency code as string, or None if parsing fails
        
    Raises:
        ValueError: If filename format is unexpected
    """
    try:
        parts = filename.split(" ")
        agency_part = parts[2].split("_")[0]
        return agency_part if agency_part else None
    except (IndexError, AttributeError) as e:
        logger.error(f"Failed to extract agency code from {filename}: {e}")
        raise ValueError(f"Invalid file name format: {filename}") from e
```

### Commit Message Convention

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`

Example:
```
feat(parse): add support for new filename format

- Update parsing logic to handle format: Holdings_AGENCYCODE-DATE.h
- Add validation for agency code format
- Update tests to cover new format

Closes #123
Relates to: JIRA-456
```

---

## License

Unknown from repository context. **Please specify the license** (e.g., MIT, Apache 2.0, proprietary).

---

## Appendix

### A. Glossary

| Term | Definition |
|------|-----------|
| **A3** | Application/module identifier (Landmark holdings) |
| **CEE** | Central External Exchange - integration platform for agency distribution |
| **SFTP** | SSH File Transfer Protocol - secure file transfer method from Landmark |
| **SAM** | AWS Serverless Application Model - infrastructure as code framework |
| **ASL** | Amazon States Language - JSON-based state machine definition |
| **Landmark** | External SFTP server hosting holdings files |
| **Agency Code** | Numerical identifier (e.g., "12345") extracted from file name |
| **Holdings** | Financial data files to be distributed to agencies |
| **Techone Adaptor** | Shared Lambda function for SFTP protocol abstraction |
| **SF Adaptor** | Shared Lambda function for Salesforce integration |

### B. File Name Parse Examples

| Filename | Parsing Steps | Agency Code | Status |
|----------|---------------|------------|--------|
| `Holdings_2024-01-15 0930 12345_SEVNET.h` | Split by space → [2]="12345_SEVNET.h" → Split by "_" → [0]="12345" | 12345 | ✅ Success |
| `Holdings_2024-01-15 0930 99999_TEST.h` | Same process | 99999 | ✅ Success |
| `Holdings_2024-01-15 0930 _INVALID.h` | Split results in empty [0] | (empty) | ❌ Failure |
| `InvalidName.h` | Split fails (not enough parts) | (error) | ❌ Failure |

### C. AWS Resource Reference

```
├── Lambda Functions
│   ├── arn:aws:lambda:ap-southeast-2:ACCOUNT:function:seil-r2dev-a3-LandmarkSFTPFileTransferFunction
│   ├── arn:aws:lambda:ap-southeast-2:ACCOUNT:function:seil-r2dev-a3-SCHEventReceiverFunction
│   ├── arn:aws:lambda:ap-southeast-2:ACCOUNT:function:seil-r2dev-a3-ParseHoldingFileFunction
│   ├── arn:aws:lambda:ap-southeast-2:ACCOUNT:function:seil-r2dev-a3-ZipHoldingFileFunction
│   ├── arn:aws:lambda:ap-southeast-2:ACCOUNT:function:seil-r2dev-a3-GetAgencyIdFromSFFunction
│   ├── arn:aws:lambda:ap-southeast-2:ACCOUNT:function:seil-r2dev-a3-SendFilesToAgenciesFunction
│   └── arn:aws:lambda:ap-southeast-2:ACCOUNT:function:seil-r2dev-a3-RejectedFileFunction
│
├── State Machine
│   └── arn:aws:states:ap-southeast-2:ACCOUNT:stateMachine:r2dev-lmk-to-agent-holding-file
│
├── S3 Buckets
│   ├── s3://seil-r2dev-holdings-file/
│   ├── s3://seil-r2dev-holdings-file-zip/
│   └── s3://seil-r2dev-holdings-file-useraccess/
│
├── CloudWatch
│   ├── /aws/lambda/seil-r2dev-a3-*
│   └── /aws/seil/r2dev/a3
│
├── IAM Roles
│   ├── seil-r2dev-a3-LandmarkSFTPFileTransferFunctionRole
│   ├── seil-r2dev-a3-SCHEventReceiverFunctionRole
│   └── ... (one per function)
│
└── EventBridge
    └── r2dev-A3-Holding-File-Scheduler (every 10 minutes)
```

### D. Key API Interactions

**Landmark → S3 (via Techone Adaptor)**:
```
Request:
{
  "sftp_path": "/ftp.out/DATAEXTRACT/DATA",
  "s3_bucket": "seil-r2dev-holdings-file",
  "log_group": "...",
  "log_stream": "..."
}

Response:
{
  "status": "success",
  "files_downloaded": 5,
  "s3_keys": [...]
}
```

**File Parsing**:
```
Request:
{
  "filedetails": [{
    "bucket": "seil-r2dev-holdings-file",
    "name": "Holdings_2024-01-15 0930 12345_SEVNET.h",
    "error_bucket": "seil-r2dev-holdings-file-useraccess",
    "id": "request-id-123"
  }]
}

Response: (Success)
{
  "status": "success",
  "bucket": "seil-r2dev-holdings-file",
  "file": "Holdings_2024-01-15 0930 12345_SEVNET.h",
  "id": "request-id-123",
  "agency_code": "12345"
}

Response: (Failure)
{
  "status": "failure",
  "bucket": "seil-r2dev-holdings-file-useraccess",
  "copySource": "seil-r2dev-holdings-file/Holdings_...",
  "target_key": "Holdings_2024-01-15 0930 12345_SEVNET.h"
}
```

**CEE Integration Message**:
```json
{
  "id": "request-id-123",
  "body": {
    "externalHoldingsId": "12345",
    "type": "Holdings",
    "files": [
      {
        "path": "s3://seil-r2dev-holdings-file-zip/Holdings_2024-01-15 0930 12345_SEVNET.h.zip"
      }
    ]
  }
}
```

### E. Monitoring & Alerting

**CloudWatch Metrics to Monitor**:
- Lambda Duration (target: < 30s for most functions)
- Lambda Error Rate (target: < 1%)
- Lambda Throttles (target: 0)
- State Machine Execution Failures (target: 0)
- S3 Put/Get errors

**Sample CloudWatch Query**:
```
fields @timestamp, @message, @duration, @billedDuration
| filter @message like /ERROR|exception/
| stats count() as error_count, avg(@duration) as avg_duration by ispresent(@error)
```

### F. Deployment Checklist

Before deploying to production:

- [ ] Code reviewed and approved
- [ ] All unit tests passing
- [ ] Code quality checks pass (Black, Bandit, Safety)
- [ ] SAM template validates without warnings
- [ ] Secrets and parameters created in target environment
- [ ] IAM roles and policies reviewed
- [ ] S3 bucket lifecycle policies configured
- [ ] CloudWatch log retention set to 30 days
- [ ] Alerts configured in CloudWatch
- [ ] Runbook documentation updated
- [ ] Rollback plan documented
- [ ] Stakeholders notified

---

## Open Questions

The following items require clarification for complete documentation:

1. **License**: What is the licensing model for this project? (MIT, Apache 2.0, proprietary, etc.)

2. **Shared Adaptor Functions**: The code references shared Lambda functions:
   - Techone SFTP Adaptor (`lmk-sftp-adaptor`)
   - Salesforce Adaptor (`sf-adaptor`)
   - CEE Request Receiver State Machine
   - Where are these defined? In a separate repository/layer?

3. **File Format Specifications**: 
   - What is the exact specification for holding files (.h format)?
   - Are there examples of valid file names and contents?

4. **CEE Platform Documentation**: 
   - What SWM_External_Holdings_ID field represents?
   - What is the contract/API spec for CEE integration?
   - What are expected response structures?

5. **Business Rules**: 
   - What should happen to files older than X days in the ZIP bucket?
   - Are there retry policies beyond the 3 state machine retries?
   - How long should files remain in rejected bucket?

6. **Monitoring &Alerting**: 
   - What are the SLA targets (latency, availability)?
   - Who should be alerted on failures?
   - What is the escalation procedure?

7. **Testing**: 
   - Are there integration tests with actual Landmark/Salesforce instances?
   - What test data / sandbox agencies are available?

8. **Deployment**: 
   - Is automated deployment to production enabled?
   - Who has approval authority for prod deployments?
   - What is the change window policy?

9. **Troubleshooting**: 
   - Is there a runbook for common issues?
   - Who is on-call for this system?
   - What is the support escalation procedure?

10. **Performance**: 
    - What is the expected throughput (files/hour)?
    - Are there performance optimization opportunities?
    - Has this been load tested?

---

**Document Version**: 1.0  
**Last Updated**: February 24, 2026  
**Author**: GitHub Copilot  
**Status**: Initial Comprehensive Documentation  

---

**For questions or updates to this documentation, please:**
1. File an issue in the repository
2. Submit a pull request with changes
3. Review with the SWM team
4. Update version number and timestamp
