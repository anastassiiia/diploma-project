// ====================== LB ALGORITHM SWITCHER ======================
let selected = 'leastconn';

function selectAlgo(btn) {
    document.querySelectorAll('.algo-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    selected = btn.dataset.algo;
    document.getElementById('hint').textContent = 'Натисніть «Застосувати» щоб змінити конфігурацію Ingress';
    hideStatus();
}

function hideStatus() {
    document.getElementById('status-msg').className = 'status-msg';
}

async function applyAlgo() {
    const btn = document.getElementById('apply-btn');
    const spinner = document.getElementById('spinner');
    const icon = document.getElementById('apply-icon');
    const label = document.getElementById('apply-label');
    const hint = document.getElementById('hint');

    btn.disabled = true;
    spinner.style.display = 'block';
    icon.style.display = 'none';
    label.textContent = 'Оновлення...';
    hint.textContent = 'Переналаштовуємо Ingress та HAProxy...';
    hideStatus();

    try {
        const resp = await fetch('/api/set-lb-algorithm', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ algorithm: selected })
        });
        const data = await resp.json();

        if (resp.ok && data.success) {
            showStatus('success', `Метод успішно змінено на «${selected}». Можна починати тест.`);
            document.getElementById('current-label').textContent = selected;
            hint.textContent = 'Готово — HAProxy перезавантажено з новими правилами.';
        } else {
            showStatus('error', data.error || 'Помилка при зміні алгоритму');
            hint.textContent = 'Спробуйте ще раз';
        }
    } catch (e) {
        showStatus('error', 'Не вдалося з\'єднатися з сервером: ' + e.message);
        hint.textContent = 'Перевірте логі контролера';
    }

    btn.disabled = false;
    spinner.style.display = 'none';
    icon.style.display = '';
    label.textContent = 'Застосувати';
}

function showStatus(type, text) {
    const el = document.getElementById('status-msg');
    const icon = document.getElementById('status-icon');
    el.className = 'status-msg ' + type;
    icon.className = type === 'success' ? 'ti ti-check' : 'ti ti-alert-circle';
    document.getElementById('status-text').textContent = text;
}


// ====================== CHARTS ======================
const ctx = document.getElementById('loadChart').getContext('2d');
const chart = new Chart(ctx, {
    type: 'line',
    data: {
        labels: [], datasets: [
            {
                label: 'Cluster CLS',
                data: [],
                borderColor: '#38bdf8',
                tension: 0.3,
                fill: true,
                backgroundColor: 'rgba(56,189,248,0.1)',
                pointRadius: 0
            },
            {
                label: 'Replicas',
                data: [],
                borderColor: '#d97706',
                yAxisID: 'y1',
                pointRadius: 0,
                tension: 0.1,
                stepped: true
            }
        ]
    },
    options: {
        maintainAspectRatio: false,
        responsive: true,
        animation: false,
        scales: {
            y:  { min: 0, max: 1,  grid: { color: '#e2e8f0' }, ticks: { color: '#64748b' } },
            y1: { position: 'right', grid: { display: false }, ticks: { color: '#d97706' }, min: 0, max: 35 },
            x:  { grid: { display: false }, ticks: { display: false } }
        },
        plugins: { legend: { labels: { color: '#1e293b' } } }
    }
});

const distCtx = document.getElementById('distChart').getContext('2d');
const distChart = new Chart(distCtx, {
    type: 'bar',
    data: {
        labels: [], datasets: [{
            label: 'Сесії на сервер',
            data: [],
            backgroundColor: '#38bdf8',
            borderRadius: 4
        }]
    },
    options: {
        maintainAspectRatio: false,
        responsive: true,
        animation: false,
        scales: {
            y: { grid: { color: '#e2e8f0' }, ticks: { color: '#64748b' } },
            x: { ticks: { color: '#94a3b8', font: { size: 10 } } }
        },
        plugins: { legend: { display: false } }
    }
});


// ====================== WEIGHTS PANEL ======================
function renderWeights(weightsData) {
    const container = document.getElementById('weights-container');
    if (!weightsData || !weightsData.weights || Object.keys(weightsData.weights).length === 0) {
        container.innerHTML = '<span style="font-size:12px;color:#64748b;">Очікування даних від контролера...</span>';
        return;
    }

    const weights = weightsData.weights;
    const clsValues = weightsData.cls_values || {};
    const maxWeight = Math.max(...Object.values(weights), 1);

    container.innerHTML = Object.entries(weights).map(([pod, w]) => {
        const shortName = pod.split('-').slice(-2).join('-');
        const cls = clsValues[pod] !== undefined ? parseFloat(clsValues[pod]).toFixed(3) : '—';
        const pct = Math.round((w / maxWeight) * 100);
        return `
            <div class="weight-row">
                <span style="min-width:80px;color:#1e293b;font-size:11px;">${shortName}</span>
                <div class="weight-bar-wrap">
                    <div class="weight-bar" style="width:${pct}%"></div>
                </div>
                <span class="weight-label">${w}</span>
                <span style="font-size:10px;color:#64748b;min-width:48px;">CLS ${cls}</span>
            </div>`;
    }).join('');
}


// ====================== CIRCUIT BREAKER PANEL ======================
function renderCircuitBreakers(cbData, pods) {
    const countEl = document.getElementById('cb-count');
    const container = document.getElementById('cb-container');

    const cbPodNames = Object.keys(cbData || {});
    const errorPods = (pods || []).filter(p => p.status === 'Metrics Error');

    if (cbPodNames.length === 0 && errorPods.length === 0) {
        countEl.className = 'badge bg-success';
        countEl.textContent = '0 активних';
        container.innerHTML = '<span style="font-size:13px;color:#64748b;">Всі поди в ротації — Circuit Breaker не активний</span>';
        return;
    }

    countEl.className = 'badge bg-danger';
    countEl.textContent = cbPodNames.length + ' активних';

    const cbHtml = cbPodNames.map(podName => {
        const secs = cbData[podName];
        const shortName = podName.split('-').pop();
        const timeStr = secs < 60 ? `${secs}с` : `${Math.floor(secs/60)}хв ${secs%60}с`;
        return `<span class="cb-pod">
            <i class="ti ti-alert-triangle" aria-hidden="true"></i>
            ${shortName} — CB відкритий ${timeStr} тому
        </span>`;
    }).join('');

    const errHtml = errorPods.map(p =>
        `<span class="cb-pod cb-pod-warn">
            <i class="ti ti-wifi-off" aria-hidden="true"></i>
            ${p.name.split('-').pop()} — метрики недоступні
        </span>`
    ).join('');

    container.innerHTML = cbHtml + errHtml;
}


// ====================== SCALING EVENTS TABLE ======================
async function updateScalingEvents() {
    try {
        const resp = await fetch('/api/scaling-events');
        const events = await resp.json();
        const tbody = document.getElementById('events-tbody');

        if (!events || events.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#64748b;padding:1rem;">Очікування подій масштабування...</td></tr>';
            return;
        }

        tbody.innerHTML = [...events].reverse().map(e => {
            const isOut = e.event === 'SCALE_OUT';
            const ts = e.timestamp ? e.timestamp.substring(11, 19) : '—';
            return `<tr>
                <td>${ts}</td>
                <td class="${isOut ? 'event-out' : 'event-in'}">${isOut ? '↑ SCALE OUT' : '↓ SCALE IN'}</td>
                <td>${e.replicas_before}</td>
                <td>${e.replicas_after}</td>
                <td>${parseFloat(e.max_cls || 0).toFixed(3)}</td>
                <td>${isOut ? '+' : '-'}${Math.abs(e.step || 1)}</td>
                <td>${parseFloat(e.reaction_time_sec || 0).toFixed(1)}с</td>
            </tr>`;
        }).join('');
    } catch (e) {
        console.error('Events fetch error:', e);
    }
}

function downloadCSV() {
    window.open('/api/scaling-events', '_blank');
}

async function clearScalingLog() {
    const btn = document.getElementById('clear-log-btn');
    if (!confirm('Очистити весь лог подій масштабування?')) return;

    btn.disabled = true;
    btn.innerHTML = '<i class="ti ti-loader-2" aria-hidden="true"></i> Очищення...';

    try {
        const resp = await fetch('/api/scaling-events/clear', { method: 'POST' });
        if (resp.ok) {
            document.getElementById('events-tbody').innerHTML =
                '<tr><td colspan="7" style="text-align:center;color:#64748b;padding:1rem;">Лог очищено</td></tr>';
        } else {
            alert('Помилка при очищенні логу');
        }
    } catch (e) {
        alert('Помилка з\'єднання: ' + e.message);
    }

    btn.disabled = false;
    btn.innerHTML = '<i class="ti ti-trash" aria-hidden="true"></i> Очистити';
}


// ====================== PODS RENDER ======================
function statusBadgeClass(status) {
    switch(status) {
        case 'Running':       return 'bg-success';
        case 'Pending':       return 'bg-warning';
        case 'Terminating':   return 'bg-secondary';
        case 'Metrics Error': return 'bg-danger';
        default:              return 'bg-secondary';
    }
}

function renderPods(pods) {
    const container = document.getElementById('pods-container');
    container.innerHTML = (pods || []).map(pod => `
        <div class="col-md-4 mb-3">
            <div class="card pod-card status-${pod.status.replace(' ', '_')} shadow-sm p-3">
                <div class="d-flex justify-content-between align-items-center mb-2">
                    <strong style="font-size:0.9rem;">${pod.name.split('-').pop()}</strong>
                    <span class="badge ${statusBadgeClass(pod.status)}" style="font-size:0.7rem;">${pod.status}</span>
                </div>
                <div class="small text-muted mb-2" style="font-size:0.75rem;">${pod.ip}</div>
                <div class="d-flex justify-content-between small mb-1">
                    <span>Load (CLS)</span><span>${pod.cls}</span>
                </div>
                <div class="cls-bar mb-3">
                    <div class="cls-fill" style="width:${Math.min(pod.cls * 100, 100)}%"></div>
                </div>
                <div class="row text-center g-0 border-top border-secondary pt-2 mt-2" style="font-size:0.8rem;">
                    <div class="col-4 border-end border-secondary">
                        <div class="text-muted small">ART</div>
                        <div>${pod.art}ms</div>
                    </div>
                    <div class="col-4 border-end border-secondary">
                        <div class="text-muted small">Success</div>
                        <div class="${pod.er > 0 ? 'error-text' : 'success-text'}">${(100 - pod.er).toFixed(1)}%</div>
                    </div>
                    <div class="col-4">
                        <div class="text-muted small">Queue</div>
                        <div>${pod.qd}</div>
                    </div>
                </div>
            </div>
        </div>`).join('');
}


// ====================== MAIN UPDATE ======================
function updateData() {
    fetch('/api/data').then(res => res.json()).then(data => {

        // Stats
        document.getElementById('total-replicas').innerText = data.total_replicas;
        document.getElementById('avg-cls').innerText = data.avg_cls;
        document.getElementById('avg-art').innerText = data.avg_art;
        document.getElementById('success-rate').innerText = data.success_rate + '%';

        if (data.success_rate < 95) {
            document.getElementById('success-rate').className = 'error-text';
            document.getElementById('status-badge').className = 'badge bg-danger';
            document.getElementById('status-badge').innerText = 'High Error Rate';
        } else {
            document.getElementById('success-rate').className = 'success-text';
            document.getElementById('status-badge').className = 'badge bg-success';
            document.getElementById('status-badge').innerText = 'System Healthy';
        }

        // Sync current algo label
        if (data.current_algo) {
            document.getElementById('current-label').textContent = data.current_algo;
        }

        // CLS Chart
        chart.data.labels = data.history.labels;
        chart.data.datasets[0].data = data.history.cls_values;
        chart.data.datasets[1].data = data.history.replicas;
        chart.update('none');

        // Distribution Chart (HAProxy Stats)
        const noDataEl = document.getElementById('haproxy-no-data');
        if (data.haproxy_stats && Object.keys(data.haproxy_stats).length > 0) {
            noDataEl.style.display = 'none';
            distChart.data.labels = Object.keys(data.haproxy_stats).map(k => k.substring(0, 12));
            distChart.data.datasets[0].data = Object.values(data.haproxy_stats);
            distChart.update('none');
        } else {
            noDataEl.style.display = 'block';
        }

        // Circuit Breakers
        renderCircuitBreakers(data.circuit_breakers || {}, data.pods || []);

        // Pods
        renderPods(data.pods || []);

    }).catch(e => console.error('Data fetch error:', e));
}

// Weights оновлюємо кожні 5с
function updateWeights() {
    fetch('/api/pod-weights').then(r => r.json()).then(renderWeights).catch(() => {});
}

// Scaling events — кожні 10с
function updateEvents() {
    updateScalingEvents();
}

setInterval(updateData, 2000);
setInterval(updateWeights, 5000);
setInterval(updateEvents, 10000);

updateData();
updateWeights();
updateScalingEvents();
