from kubernetes import client, config
import time
import requests
import csv
import os
import json
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from prometheus_client import start_http_server, Gauge
from collections import deque

# Метрики Prometheus
metric_cls_pod = Gauge('diploma_cls_pod_current', 'Predicted CLS for specific pod', ['pod_name'])
metric_cls_max = Gauge('diploma_cls_cluster_max', 'Maximum predicted CLS in cluster')
metric_replicas = Gauge('diploma_active_replicas', 'Current number of deployment replicas')

# Параметри масштабування
MAX_QD_THRESHOLD  = 50.0
MAX_ART_THRESHOLD = 2000.0
MAX_REPLICAS = 12
MIN_REPLICAS = 2

# Порогові значення CLS (гістерезисна пара)
THETA_SCALE = 0.50
THETA_DOWN  = 0.20

# Параметри методу Хольта (Double Exponential Smoothing)
ALPHA   = 0.45  # коефіцієнт згладжування рівня
BETA    = 0.35  # коефіцієнт згладжування тренду
HORIZON = 2     # горизонт прогнозування (кроків)

# Захисні інтервали між операціями масштабування
SCALE_OUT_COOLDOWN_SEC = 30
SCALE_IN_COOLDOWN_SEC  = 90

# Параметри Circuit Breaker
CB_ERROR_THRESHOLD = 50.0   # поріг частоти помилок для розмикання (%)
CB_RECOVERY_SEC    = 60     # час до автоматичного відновлення (с)

# Тривалість дренажного інтервалу перед зменшенням реплік
DRAIN_WAIT_SEC = 15

# Шляхи до файлів журналів
LOG_FILE      = "/app/logs/scaling_events.csv"
METRICS_FILE  = "/app/logs/metrics_history.csv"
WEIGHTS_FILE  = "/app/logs/pod_weights.json"

MAX_SCALING_LOG_LINES = 500
MAX_METRICS_LOG_LINES = 2000

# Глобальний стан контролера
cls_level           = {}
cls_trend           = {}
scale_down_counter  = 0
last_scale_out_time = 0
last_scale_in_time  = 0
circuit_broken_pods = {}   # {pod_name: timestamp розмикання}
drain_in_progress   = False
drain_start_time    = 0

# In-memory черги журналів з обмеженим розміром
_scaling_log_lock  = threading.Lock()
_metrics_log_lock  = threading.Lock()
_scaling_log_deque = deque(maxlen=MAX_SCALING_LOG_LINES)
_metrics_log_deque = deque(maxlen=MAX_METRICS_LOG_LINES)

SCALING_LOG_FIELDNAMES = [
    'timestamp', 'event', 'replicas_before', 'replicas_after',
    'max_cls', 'step', 'reaction_time_sec'
]
METRICS_LOG_FIELDNAMES = [
    'timestamp', 'replicas', 'max_cls', 'avg_art',
    'avg_error_rate', 'active_circuit_breakers'
]


# ---------------------------------------------------------------------------
# HTTP-сервер для надання журналів візуалізатору (порт 8002)
# ---------------------------------------------------------------------------

class LogHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        try:
            if self.path == '/logs/scaling-events':
                self._serve_deque(_scaling_log_lock, _scaling_log_deque)
            elif self.path == '/logs/metrics-history':
                self._serve_deque(_metrics_log_lock, _metrics_log_deque)
            elif self.path == '/logs/pod-weights':
                self._serve_json_file(WEIGHTS_FILE)
            elif self.path == '/logs/circuit-breakers':
                self._serve_circuit_breakers()
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())

    def do_POST(self):
        if self.path == '/logs/scaling-events/clear':
            with _scaling_log_lock:
                _scaling_log_deque.clear()
                try:
                    with open(LOG_FILE, 'w', newline='') as f:
                        csv.DictWriter(f, fieldnames=SCALING_LOG_FIELDNAMES).writeheader()
                except Exception:
                    pass
            self.send_response(204)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def _serve_circuit_breakers(self):
        now = time.time()
        result = {pod: round(now - ts) for pod, ts in circuit_broken_pods.items()}
        self._json_response(result)

    def _serve_deque(self, lock, dq):
        with lock:
            rows = list(dq)
        self._json_response(rows)

    def _serve_json_file(self, filepath):
        if not os.path.exists(filepath):
            self._json_response({})
            return
        with open(filepath, 'r') as f:
            self._json_response(json.load(f))

    def _json_response(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _start_log_server():
    HTTPServer(('0.0.0.0', 8002), LogHandler).serve_forever()


# ---------------------------------------------------------------------------
# Журналювання
# ---------------------------------------------------------------------------

def ensure_log_dir():
    os.makedirs("/app/logs", exist_ok=True)
    _load_log(LOG_FILE, SCALING_LOG_FIELDNAMES, _scaling_log_lock, _scaling_log_deque)
    _load_log(METRICS_FILE, METRICS_LOG_FIELDNAMES, _metrics_log_lock, _metrics_log_deque)


def _load_log(filepath, fieldnames, lock, dq):
    """Завантажує наявний CSV-журнал в оперативну пам'ять при старті."""
    if not os.path.exists(filepath):
        with open(filepath, 'w', newline='') as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()
        return
    try:
        with open(filepath, 'r') as f:
            rows = list(csv.DictReader(f))
        with lock:
            for row in rows[-dq.maxlen:]:
                dq.append(row)
    except Exception as e:
        print(f"[LOG LOAD ERROR] {filepath}: {e}")


def _write_log_row(filepath, fieldnames, lock, dq, row):
    """
    Атомарний запис рядка в журнал.
    Запис відбувається спочатку в оперативну чергу, потім атомарно
    на диск через тимчасовий файл з подальшою заміною (os.replace).
    """
    with lock:
        dq.append(row)
        snapshot = list(dq)
    try:
        tmp = filepath + '.tmp'
        with open(tmp, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(snapshot)
        os.replace(tmp, filepath)
    except Exception as e:
        print(f"[LOG WRITE ERROR] {filepath}: {e}")


def log_scaling_event(event, replicas_before, replicas_after, max_cls, step, reaction_time):
    _write_log_row(LOG_FILE, SCALING_LOG_FIELDNAMES, _scaling_log_lock, _scaling_log_deque, {
        'timestamp':         datetime.now().isoformat(),
        'event':             event,
        'replicas_before':   replicas_before,
        'replicas_after':    replicas_after,
        'max_cls':           round(max_cls, 4),
        'step':              step,
        'reaction_time_sec': round(reaction_time, 2),
    })


def log_metrics(replicas, max_cls, avg_art, avg_error_rate, cb_count):
    _write_log_row(METRICS_FILE, METRICS_LOG_FIELDNAMES, _metrics_log_lock, _metrics_log_deque, {
        'timestamp':               datetime.now().isoformat(),
        'replicas':                replicas,
        'max_cls':                 round(max_cls, 4),
        'avg_art':                 round(avg_art, 2),
        'avg_error_rate':          round(avg_error_rate, 2),
        'active_circuit_breakers': cb_count,
    })


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------

def _set_pod_in_rotation(v1, pod_name, namespace, active: bool):
    """
    Керує участю пода у балансуванні навантаження через маніпуляцію лейблом.

    Механізм: Service diploma-app-service використовує selector app=diploma-app.
    При розмиканні CB лейбл змінюється на app=diploma-app-disabled — под
    автоматично виключається з Endpoints і зникає з пулу HAProxy.
    При відновленні лейбл повертається до вихідного значення.
    """
    label_value = "diploma-app" if active else "diploma-app-disabled"
    try:
        v1.patch_namespaced_pod(
            name=pod_name,
            namespace=namespace,
            body={"metadata": {"labels": {"app": label_value}}}
        )
        state = "повернуто в ротацію" if active else "виключено з ротації"
        print(f"[CB] {pod_name} {state} (label app={label_value})")
    except Exception as e:
        print(f"[CB ERROR] patch labels {pod_name}: {e}")


def check_circuit_breaker(v1, namespace, pod_name, error_rate):
    """
    Перевіряє стан Circuit Breaker для заданого пода.

    При перевищенні CB_ERROR_THRESHOLD под виводиться з пулу HAProxy
    шляхом зміни лейблу. Після закінчення CB_RECOVERY_SEC лейбл
    відновлюється і под повертається в ротацію автоматично.
    """
    now = time.time()

    if pod_name in circuit_broken_pods:
        elapsed = now - circuit_broken_pods[pod_name]
        if elapsed > CB_RECOVERY_SEC:
            del circuit_broken_pods[pod_name]
            _set_pod_in_rotation(v1, pod_name, namespace, active=True)
            print(f"[CB RECOVER] {pod_name} після {elapsed:.0f} с")
        return

    if error_rate >= CB_ERROR_THRESHOLD:
        circuit_broken_pods[pod_name] = now
        _set_pod_in_rotation(v1, pod_name, namespace, active=False)
        print(f"[CB OPEN] {pod_name} | error_rate={error_rate:.1f}%")


# ---------------------------------------------------------------------------
# Weighted-балансування
# ---------------------------------------------------------------------------

def update_weighted_balance(pod_cls_map):
    """
    Обчислює ваги подів, обернено пропорційні до їх прогнозованого CLS.
    Діапазон ваг: [1, 256]. Результат зберігається у JSON-файл для
    відображення у веб-інтерфейсі моніторингу.
    """
    if not pod_cls_map:
        return
    try:
        weights = {
            pod: max(1, int((1.0 - min(cls, 1.0)) * 255) + 1)
            for pod, cls in pod_cls_map.items()
        }
        with open(WEIGHTS_FILE, 'w') as f:
            json.dump({
                'timestamp':  datetime.now().isoformat(),
                'weights':    weights,
                'cls_values': {k: round(v, 3) for k, v in pod_cls_map.items()},
            }, f)
        print(f"   [WEIGHTS] {weights}")
    except Exception as e:
        print(f"[WEIGHTS ERROR] {e}")


# ---------------------------------------------------------------------------
# Прогнозування CLS методом Хольта (Double Exponential Smoothing)
# ---------------------------------------------------------------------------

def calculate_predictive_cls(pod_name, qd, art, er, tu):
    """
    Обчислює прогнозоване значення CLS для пода методом подвійного
    експоненційного згладжування (метод Хольта).

    Формули оновлення:
        Level(t) = α·CLS(t) + (1−α)·(Level(t−1) + Trend(t−1))
        Trend(t) = β·(Level(t) − Level(t−1)) + (1−β)·Trend(t−1)
        CLS_pred = Level(t) + HORIZON·Trend(t)
    """
    qd_n  = min(qd  / MAX_QD_THRESHOLD,  1.0)
    art_n = min(art / MAX_ART_THRESHOLD, 1.0)
    er_n  = min(er  / 100.0,             1.0)
    tu_n  = min(tu  / 100.0,             1.0)

    cls_current = 0.35 * qd_n + 0.30 * art_n + 0.20 * er_n + 0.15 * tu_n

    if pod_name in cls_level:
        prev_level = cls_level[pod_name]
        prev_trend = cls_trend[pod_name]
        new_level  = ALPHA * cls_current + (1 - ALPHA) * (prev_level + prev_trend)
        new_trend  = BETA  * (new_level - prev_level)  + (1 - BETA)  * prev_trend
    else:
        new_level = cls_current
        new_trend = 0.0

    cls_level[pod_name] = new_level
    cls_trend[pod_name] = new_trend

    cls_predicted = new_level + HORIZON * new_trend
    print(f"   [CLS] {pod_name} | рівень={new_level:.3f} | прогноз={cls_predicted:.3f} | тренд={new_trend:.4f}")
    return max(0.0, min(cls_predicted, 1.2))


# ---------------------------------------------------------------------------
# Масштабування
# ---------------------------------------------------------------------------

def calculate_scale_step(current_replicas, max_cls):
    """
    Визначає крок масштабування вгору пропорційно до рівня перевантаження.
    Обмеження: не більше 50% від поточної кількості реплік за одну операцію.
    """
    overload_factor = max_cls / THETA_SCALE
    step = 2 + int((overload_factor - 1) * current_replicas * 0.5)
    return max(1, min(step, max(3, int(current_replicas * 0.5))))


def scale_deployment(apps_v1, namespace, deployment_name, action,
                     step=1, max_cls=0.0, reaction_start=None):
    """
    Виконує операцію горизонтального масштабування деплойменту.

    SCALE_IN реалізує процедуру безпечного виведення (дренажу):
    протягом DRAIN_WAIT_SEC секунд реплікам дозволяється завершити
    активні з'єднання перед фактичним зменшенням spec.replicas.
    """
    global drain_in_progress, drain_start_time

    try:
        scale   = apps_v1.read_namespaced_deployment_scale(
            name=deployment_name, namespace=namespace)
        current = scale.spec.replicas or 1

        if action == "SCALE_OUT" and current < MAX_REPLICAS:
            new_replicas = min(current + step, MAX_REPLICAS)
            if new_replicas > current:
                scale.spec.replicas = new_replicas
                apps_v1.patch_namespaced_deployment_scale(
                    name=deployment_name, namespace=namespace, body=scale)
                reaction_time = time.time() - reaction_start if reaction_start else 0
                print(f"[SCALE_OUT] {current} -> {new_replicas} (+{new_replicas - current})")
                log_scaling_event("SCALE_OUT", current, new_replicas,
                                  max_cls, new_replicas - current, reaction_time)
                return new_replicas

        elif action == "SCALE_IN" and current > MIN_REPLICAS:
            if not drain_in_progress:
                drain_in_progress = True
                drain_start_time  = time.time()
                print(f"[DRAIN] Початок дренажного інтервалу ({DRAIN_WAIT_SEC} с)")
                return current

            elapsed = time.time() - drain_start_time
            if elapsed < DRAIN_WAIT_SEC:
                print(f"[DRAIN] Залишилось {DRAIN_WAIT_SEC - elapsed:.0f} с")
                return current

            drain_in_progress = False
            new_replicas = max(current - 1, MIN_REPLICAS)
            scale.spec.replicas = new_replicas
            apps_v1.patch_namespaced_deployment_scale(
                name=deployment_name, namespace=namespace, body=scale)
            reaction_time = time.time() - reaction_start if reaction_start else 0
            print(f"[SCALE_IN] {current} -> {new_replicas}")
            log_scaling_event("SCALE_IN", current, new_replicas, max_cls, 1, reaction_time)
            return new_replicas

        return current
    except Exception as e:
        print(f"[ERROR] scale_deployment: {e}")
        return None


# ---------------------------------------------------------------------------
# Головний цикл керування
# ---------------------------------------------------------------------------

def main():
    global scale_down_counter, last_scale_out_time, last_scale_in_time

    ensure_log_dir()

    threading.Thread(target=_start_log_server, daemon=True).start()
    print("[INFO] HTTP-сервер журналів запущено на порту 8002")

    start_http_server(8001)

    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

    v1      = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()

    print("[INFO] Предиктивний контролер запущено")

    scale_out_trigger_time = None

    while True:
        try:
            pods = v1.list_namespaced_pod(
                namespace="default", label_selector="app=diploma-app")
            current_replicas = len(pods.items)
            metric_replicas.set(current_replicas)

            cls_values   = []
            pod_cls_map  = {}
            total_art    = 0.0
            total_er     = 0.0
            active_count = 0

            for pod in pods.items:
                if not pod.status.pod_ip or pod.status.phase != "Running":
                    continue
                try:
                    resp = requests.get(
                        f"http://{pod.status.pod_ip}:8000/metrics", timeout=2)
                    data = resp.json()

                    er  = data.get("error_rate", 0)
                    art = data.get("average_response_time", 0)

                    # Перевірка Circuit Breaker з реальним виключенням пода з пулу
                    check_circuit_breaker(v1, "default", pod.metadata.name, er)

                    cls_pred = calculate_predictive_cls(
                        pod.metadata.name,
                        data.get("request_queue_length", 0),
                        art, er,
                        data.get("thread_utilization", 0),
                    )
                    cls_values.append(cls_pred)
                    pod_cls_map[pod.metadata.name] = cls_pred
                    metric_cls_pod.labels(pod_name=pod.metadata.name).set(cls_pred)

                    total_art    += art
                    total_er     += er
                    active_count += 1
                except Exception:
                    continue

            # Очищення стану завершених подів
            active_names = {pod.metadata.name for pod in pods.items}
            for name in list(cls_level.keys()):
                if name not in active_names:
                    del cls_level[name]
                    del cls_trend[name]
            for name in list(circuit_broken_pods.keys()):
                if name not in active_names:
                    del circuit_broken_pods[name]

            update_weighted_balance(pod_cls_map)

            if cls_values:
                max_cls      = max(cls_values)
                current_time = time.time()
                avg_art = total_art / active_count if active_count > 0 else 0
                avg_er  = total_er  / active_count if active_count > 0 else 0

                metric_cls_max.set(max_cls)
                log_metrics(current_replicas, max_cls, avg_art, avg_er,
                            len(circuit_broken_pods))
                print(f"[STAT] CLS={max_cls:.3f} | реплік={current_replicas} "
                      f"| CB={len(circuit_broken_pods)}")

                # Логіка масштабування вгору
                if max_cls > THETA_SCALE:
                    if scale_out_trigger_time is None:
                        scale_out_trigger_time = current_time
                    if current_time - last_scale_out_time > SCALE_OUT_COOLDOWN_SEC:
                        step = calculate_scale_step(current_replicas, max_cls)
                        scale_deployment(apps_v1, "default", "diploma-app",
                                         "SCALE_OUT", step, max_cls, scale_out_trigger_time)
                        last_scale_out_time    = current_time
                        scale_down_counter     = 0
                        scale_out_trigger_time = None
                else:
                    scale_out_trigger_time = None

                # Логіка масштабування вниз (4 підтверджуючі ітерації)
                if max_cls < THETA_DOWN:
                    scale_down_counter += 1
                    if scale_down_counter >= 4:
                        if (current_time - last_scale_in_time  > SCALE_IN_COOLDOWN_SEC and
                                current_time - last_scale_out_time > SCALE_IN_COOLDOWN_SEC):
                            result = scale_deployment(
                                apps_v1, "default", "diploma-app",
                                "SCALE_IN", max_cls=max_cls,
                                reaction_start=current_time)
                            if result and result < current_replicas:
                                last_scale_in_time = current_time
                                scale_down_counter = 0
                else:
                    scale_down_counter = 0

            time.sleep(5)

        except Exception as e:
            print(f"[CRITICAL] {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
