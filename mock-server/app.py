from flask import Flask, jsonify, request
import json
import os
from datetime import datetime

app = Flask(__name__)

def load_customers():
    data_path = os.path.join(os.path.dirname(__file__), 'data', 'customers.json')
    try:
        with open(data_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        app.logger.error(f"Customer data file not found at {data_path}")
        return []
    except json.JSONDecodeError:
        app.logger.error("Error parsing customers.json")
        return []

CUSTOMERS = load_customers()

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "service": "flask-mock-server",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }), 200

@app.route('/api/customers', methods=['GET'])
def get_customers():
    try:
        page = request.args.get('page', default=1, type=int)
        limit = request.args.get('limit', default=10, type=int)
        
        if page < 1:
            page = 1
        if limit < 1 or limit > 100:
            limit = 10
        
        total = len(CUSTOMERS)
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        
        paginated_data = CUSTOMERS[start_idx:end_idx]
        
        return jsonify({
            "data": paginated_data,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": (total + limit - 1) // limit
        }), 200
    
    except Exception as e:
        app.logger.error(f"Error fetching customers: {str(e)}")
        return jsonify({
            "error": "Failed to fetch customers",
            "detail": str(e)
        }), 500

@app.route('/api/customers/<customer_id>', methods=['GET'])
def get_customer(customer_id):
    try:
        customer = next(
            (c for c in CUSTOMERS if c['customer_id'] == customer_id),
            None
        )
        
        if not customer:
            return jsonify({
                "error": "Customer not found",
                "customer_id": customer_id
            }), 404
        
        return jsonify({
            "data": customer,
            "status": "success"
        }), 200
    
    except Exception as e:
        app.logger.error(f"Error fetching customer {customer_id}: {str(e)}")
        return jsonify({
            "error": "Failed to fetch customer",
            "detail": str(e)
        }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Endpoint not found",
        "path": request.path,
        "method": request.method
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "error": "Internal server error",
        "detail": str(error)
    }), 500

if __name__ == '__main__':
    print(f"Loaded {len(CUSTOMERS)} customers from data/customers.json")
    app.run(host='0.0.0.0', port=5000, debug=False)
