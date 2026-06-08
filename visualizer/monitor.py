from flask import Flask, render_template, jsonify, request as flask_request
from kubernetes import client, config
import requests
import time
import subprocess
import json

app = Flask(__name__)

CURRENT_ALGO = "leastconn"
history = {"labels": [], "cls_values": [], "replicas": [], "latency": []}

CONTROLLER_LOG_URL = "http://diploma-controller-service.default.svc.cluster.local:8002"

HAPROXY_STATS_URLS = [
    "http://haproxy-ingress-kubernetes-ingress.default.svc.cluster.local:1024/stats;csv",
    "http://haproxy-ingress-kubernetes-ingress.default.svc.cluster.local/stats;csv",
]

_k8s_v1 = None


def get_k8s_v1():
    global _k8s_v1
    if _k8s_v1 is None:
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        _k8s_v1 = client.CoreV1Api()
    return _k8s_v1


def get_cluster_data():
    v1   = get_k8s_v1()
    pods = v1.list_namespaced_pod(namespace="default", label_selector="app=diploma-app")

    pod_stats  = []
    total_cls  = total_art = total_er = 0
    active_pods = 0

    for pod in pods.items:
        if pod.metadata.deletion_timestamp is not None:
            status = "Terminating"
        else:
            status = pod.status.phase or "Unknown"

        ip   = pod.status.pod_ip
        name = pod.metadata.name
        current_cls = current_art = current_er = qd = tu = 0

        if status == "Running" and ip:
            try:
                resp = requests.get(f"http://{ip}:8000/metrics", timeout=0.5)
                data = resp.json()
                qd          = data.get("request_queue_length", 0)
                tu          = data.get("thread_utilization", 0)
                current_art = data.get("average_response_time", 0)
                current_er  = data.get("error_rate", 0)
                current_cls = (qd / 50 * 0.35 + current_art / 2000 * 0.30 +
                               current_er / 100 * 0.20 + tu / 100 * 0.15)
                total_cls += current_cls
                total_art += current_art
                total_er  += current_er
                active_pods += 1
            except Exception:
                status = "Metrics Error"

        pod_stats.append({
            "name":   name,
            "status": status,
            "ip":     ip or "—",
            "cls":    round(current_cls, 3),
            "art":    round(current_art, 1),
            "er":     round(current_er, 1),
            "qd":     qd,
            "tu":     tu,
        })

    n = active_pods or 1
    return {
        "pods":           pod_stats,
        "total_replicas": len(pods.items),
        "avg_cls":        round(total_cls / n, 3),
        "avg_art":        round(total_art / n, 1),
        "success_rate":   round(100 - total_er / n, 1),
    }


def get_haproxy_stats():
    """Отримує статистику розподілу запитів з HAProxy Stats API."""
    for url in HAPROXY_STATS_URLS:
        try:
            resp = requests.get(url, timeout=2)
            if resp.status_code != 200 or "svname" not in resp.text:
                continue
            stats = {}
            for line in resp.text.strip().split('\n')[1:]:
                line  = line.lstrip('# ')
                parts = line.split(',')
                if len(parts) > 48 and parts[1] not in ('FRONTEND', 'BACKEND', ''):
                    val = parts[48]
                    stats[parts[1]] = int(val) if val.isdigit() else 0
            if stats:
                return stats
        except Exception as e:
            print(f"[HAPROXY] {url}: {e}")
    return {}


def fetch_from_controller(path, method='GET'):
    """Звертається до HTTP-сервера контролера для отримання журналів."""
    try:
        url = f"{CONTROLLER_LOG_URL}{path}"
        if method == 'POST':
            resp = requests.post(url, timeout=2)
        else:
            resp = requests.get(url, timeout=2)
        resp.raise_for_status()
        return True if method == 'POST' else resp.json()
    except requests.exceptions.ConnectionError:
        print(f"[WARN] Сервіс контролера недоступний: {path}")
        return None
    except Exception as e:
        print(f"[WARN] fetch_from_controller({method} {path}): {e}")
        return None


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/data')
def data():
    cluster_data = get_cluster_data()
    haproxy_data = get_haproxy_stats()
    cb_data      = fetch_from_controller('/logs/circuit-breakers') or {}
    curr_time    = time.strftime("%H:%M:%S")

    history["labels"].append(curr_time)
    history["cls_values"].append(cluster_data["avg_cls"])
    history["replicas"].append(cluster_data["total_replicas"])
    history["latency"].append(cluster_data["avg_art"])

    if len(history["labels"]) > 30:
        for key in history:
            history[key].pop(0)

    return jsonify({
        **cluster_data,
        "haproxy_stats":   haproxy_data,
        "current_algo":    CURRENT_ALGO,
        "history":         history,
        "circuit_breakers": cb_data,
    })


@app.route('/api/set-lb-algorithm', methods=['POST'])
def set_lb_algorithm():
    global CURRENT_ALGO
    data = flask_request.get_json()
    algo = data.get('algorithm')

    if algo not in ['roundrobin', 'leastconn', 'uri']:
        return jsonify({"success": False, "error": "Невідомий алгоритм"}), 400

    patch  = {"metadata": {"annotations": {"haproxy.org/load-balance": algo}}}
    result = subprocess.run(
        ["kubectl", "patch", "ingress", "diploma-ingress",
         "--type=merge", "-p", json.dumps(patch)],
        capture_output=True, text=True,
    )

    if result.returncode == 0:
        CURRENT_ALGO = algo
        return jsonify({"success": True, "algorithm": algo})
    return jsonify({"success": False, "error": result.stderr}), 500


@app.route('/api/scaling-events')
def scaling_events():
    data = fetch_from_controller('/logs/scaling-events')
    if data is None:
        return jsonify([])
    return jsonify(data[-20:])


@app.route('/api/scaling-events/clear', methods=['POST'])
def clear_scaling_events():
    success = fetch_from_controller('/logs/scaling-events/clear', method='POST')
    if success:
        return jsonify({"success": True})
    return jsonify({"success": False, "error": "Сервіс контролера недоступний"}), 502


@app.route('/api/metrics-history')
def metrics_history():
    rows = fetch_from_controller('/logs/metrics-history')
    if not rows:
        return jsonify({"labels": [], "cls": [], "replicas": [], "latency": [], "errors": []})

    rows   = rows[-60:]
    result = {"labels": [], "cls": [], "replicas": [], "latency": [], "errors": []}
    for row in rows:
        ts = row.get("timestamp", "")
        result["labels"].append(ts[11:19] if len(ts) > 19 else ts)
        result["cls"].append(float(row.get("max_cls", 0)))
        result["replicas"].append(int(row.get("replicas", 0)))
        result["latency"].append(float(row.get("avg_art", 0)))
        result["errors"].append(float(row.get("avg_error_rate", 0)))
    return jsonify(result)


@app.route('/api/pod-weights')
def pod_weights():
    data = fetch_from_controller('/logs/pod-weights')
    if not data or not data.get("weights"):
        return jsonify({"weights": {}, "cls_values": {}, "timestamp": None})
    return jsonify(data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True)
