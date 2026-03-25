import requests
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from models.customer import Customer
import logging

logger = logging.getLogger(__name__)

class IngestionService:
    def __init__(self, flask_url: str = "http://mock-server:5000"):
        self.flask_url = flask_url
        self.session_timeout = 10

    def fetch_all_customers_from_flask(self) -> list:
        customers = []
        page = 1
        limit = 100
        
        try:
            while True:
                url = f"{self.flask_url}/api/customers?page={page}&limit={limit}"
                logger.info(f"Fetching from: {url}")
                
                response = requests.get(url, timeout=self.session_timeout)
                response.raise_for_status()
                
                data = response.json()
                batch = data.get("data", [])
                
                if not batch:
                    break
                
                customers.extend(batch)
                
                total = data.get("total", 0)
                if len(customers) >= total:
                    break
                
                page += 1
            
            logger.info(f"Fetched {len(customers)} customers")
            return customers
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching customers: {str(e)}")
            raise

    def parse_customer_data(self, customer_data: dict) -> dict:
        from datetime import datetime as dt
        
        dob = None
        if customer_data.get("date_of_birth"):
            try:
                dob = dt.strptime(customer_data["date_of_birth"], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                logger.warning(f"Invalid DOB for {customer_data.get('customer_id')}")
        
        created_at = datetime.utcnow()
        if customer_data.get("created_at"):
            try:
                # Handle ISO format with Z
                created_str = customer_data["created_at"].replace("Z", "+00:00")
                created_at = dt.fromisoformat(created_str).replace(tzinfo=None)
            except (ValueError, AttributeError):
                logger.warning(f"Invalid created_at for {customer_data.get('customer_id')}")
        
        return {
            "customer_id": customer_data.get("customer_id"),
            "first_name": customer_data.get("first_name", ""),
            "last_name": customer_data.get("last_name", ""),
            "email": customer_data.get("email", ""),
            "phone": customer_data.get("phone"),
            "address": customer_data.get("address"),
            "date_of_birth": dob,
            "account_balance": customer_data.get("account_balance", 0),
            "created_at": created_at
        }

    def upsert_customer(self, db: Session, customer_data: dict) -> tuple:
        try:
            parsed_data = self.parse_customer_data(customer_data)
            customer_id = parsed_data["customer_id"]
            
            existing_customer = db.query(Customer).filter(
                Customer.customer_id == customer_id
            ).first()
            
            if existing_customer:
                for key, value in parsed_data.items():
                    if key != "customer_id":
                        setattr(existing_customer, key, value)
                logger.info(f"Updated {customer_id}")
                
            else:
                new_customer = Customer(**parsed_data)
                db.add(new_customer)
                logger.info(f"Created {customer_id}")
            
            db.flush()
            return True, f"Upserted {customer_id}"
        
        except IntegrityError as e:
            db.rollback()
            logger.error(f"Integrity error upserting customer: {str(e)}")
            # Handle unique constraint on email
            return False, f"Duplicate email in customer data"
        
        except Exception as e:
            db.rollback()
            logger.error(f"Error upserting customer: {str(e)}")
            return False, str(e)

    def ingest_customers(self, db: Session) -> dict:
        try:
            customers = self.fetch_all_customers_from_flask()
            
            if not customers:
                logger.warning("No customers found")
                return {
                    "status": "warning",
                    "message": "No customers to ingest",
                    "records_processed": 0
                }
            
            success_count = 0
            error_count = 0
            errors = []
            
            for customer_data in customers:
                success, message = self.upsert_customer(db, customer_data)
                if success:
                    success_count += 1
                else:
                    error_count += 1
                    errors.append(message)
            
            db.commit()
            
            logger.info(f"Ingestion complete: {success_count} successful, {error_count} errors")
            
            return {
                "status": "success",
                "records_processed": success_count,
                "records_failed": error_count,
                "total_records": len(customers),
                "errors": errors if errors else []
            }
        
        except Exception as e:
            db.rollback()
            logger.error(f"Fatal error during ingestion: {str(e)}")
            return {
                "status": "error",
                "message": str(e),
                "records_processed": 0
            }
