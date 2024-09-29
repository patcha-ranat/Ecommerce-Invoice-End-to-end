from fastapi import FastAPI

# put basemodels here

app = FastAPI()

@app.get("/")
async def hello_world():
    return {"Message": "Hello World"}

@app.get("/customer/{customer_id}")
async def get_customer(customer_id: int):
    if customer_id == 1:
        response = {
        "customer_id": customer_id,
        "name": "John Doe",
        "cluster": 1,
        "status": "active"
        }
    elif customer_id == 0:
        response = {
        "customer_id": customer_id,
        "name": "Jane Doe",
        "cluster": 0,
        "status": "inactive"
        }
    else:
        response = {
        "customer_id": customer_id,
        "name": "Unknown",
        "cluster": -1,
        "status": "unknown"
        }
    return response

# for lcoal testing
# bash: uvicorn api_app:app --reload 