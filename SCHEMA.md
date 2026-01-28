# Minnesota Business Data Schema

## Data Summary

- **Total Records:** 286,469
- **Business Types:** 12 different types
- **Date Range:** 1959 - present

## Business Type Distribution

| Type | Count | % |
|------|-------|---|
| Assumed Name | 253,916 | 88.6% |
| Business Corporation (Foreign) | 22,755 | 7.9% |
| Trademark - Service Mark | 5,153 | 1.8% |
| Trademark | 3,462 | 1.2% |
| Nonprofit Corporation (Foreign) | 571 | 0.2% |
| Other (7 types) | 612 | 0.2% |

---

## Recommended Schema (Normalized)

### Option 1: Normalized (Recommended for SQL databases)

```sql
-- Main business table
CREATE TABLE businesses (
    id SERIAL PRIMARY KEY,
    file_number BIGINT UNIQUE NOT NULL,
    business_name VARCHAR(500) NOT NULL,
    mn_statute VARCHAR(20),
    business_type VARCHAR(100),
    home_jurisdiction VARCHAR(100),
    filing_date DATE,
    status VARCHAR(50),
    renewal_due_date DATE,

    -- Type-specific fields (nullable)
    mark_type VARCHAR(50),           -- Trademarks only
    number_of_shares VARCHAR(50),    -- Corporations only
    chief_executive_officer VARCHAR(200), -- Corporations only
    manager VARCHAR(200),            -- LLCs only
    registered_agent_name VARCHAR(200),

    filing_history TEXT,
    scraped_at DATE,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Addresses table (normalized)
CREATE TABLE addresses (
    id SERIAL PRIMARY KEY,
    business_id INTEGER REFERENCES businesses(id),
    address_type VARCHAR(20) NOT NULL, -- 'principal', 'registered_office', 'executive_office', 'applicant'

    street_number VARCHAR(20),
    street_name VARCHAR(200),
    street_type VARCHAR(20),
    street_direction VARCHAR(10),
    unit VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(50),
    zip VARCHAR(20),
    address_raw TEXT,

    -- For applicant addresses
    contact_name VARCHAR(200),  -- applicant_name or markholder

    UNIQUE(business_id, address_type)
);

-- Indexes
CREATE INDEX idx_businesses_file_number ON businesses(file_number);
CREATE INDEX idx_businesses_type ON businesses(business_type);
CREATE INDEX idx_businesses_status ON businesses(status);
CREATE INDEX idx_businesses_filing_date ON businesses(filing_date);
CREATE INDEX idx_addresses_business_id ON addresses(business_id);
CREATE INDEX idx_addresses_city_state ON addresses(city, state);
CREATE INDEX idx_addresses_zip ON addresses(zip);
```

### Option 2: Flat Table (Simpler, good for analytics)

```sql
CREATE TABLE mn_businesses (
    file_number BIGINT PRIMARY KEY,
    business_name VARCHAR(500) NOT NULL,
    mn_statute VARCHAR(20),
    business_type VARCHAR(100),
    home_jurisdiction VARCHAR(100),
    filing_date DATE,
    status VARCHAR(50),
    renewal_due_date DATE,

    -- Type-specific
    mark_type VARCHAR(50),
    number_of_shares VARCHAR(50),
    chief_executive_officer VARCHAR(200),
    manager VARCHAR(200),
    registered_agent_name VARCHAR(200),

    -- Principal Address
    principal_street_number VARCHAR(20),
    principal_street_name VARCHAR(200),
    principal_street_type VARCHAR(20),
    principal_street_direction VARCHAR(10),
    principal_unit VARCHAR(50),
    principal_city VARCHAR(100),
    principal_state VARCHAR(50),
    principal_zip VARCHAR(20),
    principal_address_raw TEXT,

    -- Registered Office Address
    reg_office_street_number VARCHAR(20),
    reg_office_street_name VARCHAR(200),
    reg_office_street_type VARCHAR(20),
    reg_office_street_direction VARCHAR(10),
    reg_office_unit VARCHAR(50),
    reg_office_city VARCHAR(100),
    reg_office_state VARCHAR(50),
    reg_office_zip VARCHAR(20),
    reg_office_address_raw TEXT,

    -- Executive Office Address
    exec_office_street_number VARCHAR(20),
    exec_office_street_name VARCHAR(200),
    exec_office_street_type VARCHAR(20),
    exec_office_street_direction VARCHAR(10),
    exec_office_unit VARCHAR(50),
    exec_office_city VARCHAR(100),
    exec_office_state VARCHAR(50),
    exec_office_zip VARCHAR(20),
    exec_office_address_raw TEXT,

    -- Applicant/Markholder
    applicant_name VARCHAR(200),
    applicant_street_number VARCHAR(20),
    applicant_street_name VARCHAR(200),
    applicant_street_type VARCHAR(20),
    applicant_street_direction VARCHAR(10),
    applicant_unit VARCHAR(50),
    applicant_city VARCHAR(100),
    applicant_state VARCHAR(50),
    applicant_zip VARCHAR(20),
    applicant_address_raw TEXT,

    filing_history TEXT,
    scraped_at DATE
);
```

---

## Field Population Rates

| Field | % Populated | Notes |
|-------|-------------|-------|
| file_number | 100% | Unique identifier |
| business_name | 100% | |
| business_type | 100% | |
| filing_date | 100% | |
| status | 100% | Active/Inactive |
| filing_history | 100% | Semicolon-separated events |
| mn_statute | 99.8% | Usually 333, 303, 302A, 322C |
| home_jurisdiction | 97.0% | State of formation |
| renewal_due_date | 93.0% | |
| applicant_name | 91.6% | Person/entity who filed |
| principal_address | 88.5% | Business location |
| applicant_address | 85-91% | Applicant's address |
| registered_agent | 11.4% | Corporations only |
| reg_office_address | 6.7% | Corporations only |
| mark_type | 3.0% | Trademarks only |
| CEO | 1.7% | Corporations only |
| exec_office_address | <0.1% | Rarely populated |

---

## Business Type Details

### Assumed Name (88.6%)
- MN Statute: 333
- Has: Principal address, Applicant info
- Missing: Registered agent, CEO, mark_type

### Business Corporation Foreign (7.9%)
- MN Statute: 303
- Has: Registered office, Registered agent, sometimes CEO
- Missing: Principal address (uses reg office instead)

### Trademarks (3.0%)
- MN Statute: 333
- Has: mark_type, Markholder (stored as applicant)
- Types: Trademark, Service Mark, Collective Mark, Certification Mark

---

## Import Script Example

```python
import pandas as pd
import psycopg2

# Read CSV
df = pd.read_csv('businesses_all.csv', low_memory=False)

# Convert dates
df['filing_date'] = pd.to_datetime(df['filing_date'], errors='coerce')
df['renewal_due_date'] = pd.to_datetime(df['renewal_due_date'], errors='coerce')
df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')

# Connect and insert
conn = psycopg2.connect("dbname=mn_businesses user=...")
df.to_sql('mn_businesses', conn, if_exists='replace', index=False)
```
