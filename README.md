# Backend Assessment

A data pipeline that pulls customer data from a Flask server, transforms it, and loads it into PostgreSQL. The FastAPI service handles the ETL logic.

## Getting Started

```bash
docker-compose up -d
sleep 15
curl -X POST http://localhost:8000/api/ingest
```

That's it. Flask runs on 5000, FastAPI on 8000, and PostgreSQL on 5432. Give it 15 seconds to start, then hit the ingest endpoint.

## What's Running

- **Flask** (port 5000) - Serves customer data from a JSON file
- **FastAPI** (port 8000) - Ingests the data and stores it in the database
- **PostgreSQL** (port 5432) - Where the data lives. Database is `customer_db`, user is `postgres`, password is `password`

## API Endpoints

Flask has the mock data:
- `GET /api/health` - Health check
- `GET /api/customers?page=1&limit=10` - List customers
- `GET /api/customers/CUST001` - Get one customer

FastAPI does the pipeline work:
- `GET /api/health` - Health check
- `POST /api/ingest` - Pull data from Flask and load into the database
- `GET /api/customers?page=1&limit=10` - Query from the database
- `GET /api/customers/CUST001` - Get one customer from the database
- `GET /api/stats` - Get balance statistics

There are 21 customers in the dataset.

## Testing It Out

Check Flask is working:
```bash
curl http://localhost:5000/api/customers?page=1&limit=5
```

Then ingest the data:
```bash
curl -X POST http://localhost:8000/api/ingest
```

Query from the database:
```bash
curl http://localhost:8000/api/customers?page=1&limit=5
```

## Cleanup

Stop everything:
```bash
docker-compose down
```

Reset the database:
```bash
docker-compose down -v
docker-compose up -d
```
