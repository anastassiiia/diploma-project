from fastapi import FastAPI, Request, HTTPException
import time
import asyncio

app = FastAPI(title="Diploma Microservice")

active_connections   = 0
request_queue_length = 0
total_requests       = 0
total_latency        = 0.0
error_count          = 0
MAX_THREADS          = 40
metrics_lock         = asyncio.Lock()


@app.middleware("http")
async def monitor_requests(request: Request, call_next):
    global active_connections, request_queue_length, total_requests, total_latency, error_count

    if request.url.path == "/metrics":
        return await call_next(request)

    request_queue_length += 1
    active_connections   += 1
    start_time = time.time()
    try:
        response = await call_next(request)
        latency  = (time.time() - start_time) * 1000
        async with metrics_lock:
            total_requests += 1
            total_latency  += latency
            if response.status_code >= 500:
                error_count += 1
        return response
    except Exception:
        async with metrics_lock:
            error_count += 1
        raise
    finally:
        request_queue_length -= 1
        active_connections   -= 1


@app.get("/")
async def root():
    return {"status": "healthy"}


@app.get("/heavy-task")
async def heavy_task(delay: float = 1.2):
    """Імітація тривалого запиту для навантажувального тестування."""
    await asyncio.sleep(delay)
    return {"message": "completed", "delay": delay}


@app.get("/metrics")
async def get_metrics():
    """Повертає поточні метрики навантаження та скидає лічильники."""
    async with metrics_lock:
        current_reqs  = total_requests
        current_lat   = total_latency
        current_errs  = error_count
        globals()['total_requests'] = 0
        globals()['total_latency']  = 0.0
        globals()['error_count']    = 0

    avg_art  = (current_lat / current_reqs)         if current_reqs > 0 else 0.0
    err_rate = (current_errs / current_reqs * 100)  if current_reqs > 0 else 0.0
    tu       = min((active_connections / MAX_THREADS) * 100, 100.0)

    return {
        "request_queue_length":  request_queue_length,
        "average_response_time": round(avg_art, 2),
        "error_rate":            round(err_rate, 2),
        "thread_utilization":    round(tu, 2),
    }


@app.get("/error-test")
async def error_test():
    """Тестовий ендпоінт для перевірки Circuit Breaker."""
    raise HTTPException(status_code=500, detail="Simulated error")
