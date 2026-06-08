FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir locust==2.29.1

COPY locustfile.py .

EXPOSE 8089

# Запускаємо з веб-інтерфейсом і графіками
CMD ["locust", \
     "--host", "http://haproxy-ingress-kubernetes-ingress.default.svc.cluster.local", \
     "--web-port", "8089", \
     "--web-host", "0.0.0.0", \
     "--users", "0", \
     "--spawn-rate", "1"]
