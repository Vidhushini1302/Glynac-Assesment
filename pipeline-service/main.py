from fastapi import FastAPI, Query, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import logging

from database import init_db, get_db, engine, SessionLocal
from models.customer import Customer
from services.ingestion import IngestionService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Data Ingestion Pipeline",
    description="Ingests customer data from Flask into PostgreSQL",
    version="1.0.0"
)

ingestion_service = IngestionService(flask_url="http://mock-server:5000")
class CustomerResponse(BaseModel):
    customer_id: str
    first_name: str
    last_name: str
    email: str
    phone: Optional[str] = None
    address: Optional[str] = None
    date_of_birth: Optional[str] = None
    account_balance: float
    created_at: Optional[str] = None

    class Config:
        from_attributes = True

class CustomersListResponse(BaseModel):
    data: List[CustomerResponse]
    total: int
    page: int
    limit: int
    total_pages: int

class IngestionResponse(BaseModel):
    status: str
    records_processed: int
    records_failed: Optional[int] = 0
    total_records: Optional[int] = 0
    errors: Optional[List[str]] = []

class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str

@app.on_event("startup")
async def startup():
    try:
        logger.info("Initializing database")
        init_db()
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    return {
        "status": "healthy",
        "service": "fastapi-pipeline",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

@app.post("/api/ingest", response_model=IngestionResponse)
async def ingest_customers(background_tasks: BackgroundTasks):
    db = SessionLocal()
    try:
        result = ingestion_service.ingest_customers(db)
        
        return {
            "status": result.get("status"),
            "records_processed": result.get("records_processed", 0),
            "records_failed": result.get("records_failed", 0),
            "total_records": result.get("total_records", 0),
            "errors": result.get("errors", [])
        }
    
    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
    finally:
        db.close()

@app.get("/api/customers", response_model=CustomersListResponse)
async def get_customers(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    try:
        offset = (page - 1) * limit
        total = db.query(Customer).count()
        
        customers = db.query(Customer)\
            .order_by(Customer.created_at)\
            .offset(offset)\
            .limit(limit)\
            .all()
        
        data = [CustomerResponse.from_orm(customer) for customer in customers]
        
        return {
            "data": data,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": (total + limit - 1) // limit
        }
    
    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Database error occurred"
        )
    except Exception as e:
        logger.error(f"Error fetching customers: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.get("/api/customers/{customer_id}", response_model=CustomerResponse)
async def get_customer(
    customer_id: str,
    db: Session = Depends(get_db)
):
    try:
        customer = db.query(Customer).filter(
            Customer.customer_id == customer_id
        ).first()
        
        if not customer:
            raise HTTPException(
                status_code=404,
                detail=f"Customer with id {customer_id} not found"
            )
        
        return CustomerResponse.from_orm(customer)
    
    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Database error occurred"
        )
    except Exception as e:
        logger.error(f"Error fetching customer {customer_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.get("/api/stats")
async def get_statistics(db: Session = Depends(get_db)):
    """Get statistics about customers in database"""
    try:
        total_customers = db.query(Customer).count()
        total_balance = db.query(Customer).with_entities(
            db.func.sum(Customer.account_balance)
        ).scalar() or 0
        
        return {
            "total_customers": total_customers,
            "total_balance": float(total_balance),
            "average_balance": float(total_balance / total_customers) if total_customers > 0 else 0
        }
    
    except Exception as e:
        logger.error(f"Error fetching statistics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions"""
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc)
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
