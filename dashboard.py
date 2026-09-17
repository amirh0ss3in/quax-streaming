import io, json, threading
from http.server import HTTPServer, BaseHTTPRequestHandler

import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from kafka import KafkaConsumer

PORT, BOOTSTRAP = 8000, ['10.67.22.111:9092']
state = {}                                   # latest batch + running average

## NOTE:
## This code is relatively simple. 
## We have two threads that run continuously, so we use threading to run them concurrently.
## One thread continuously consumes messages from Kafka and updates the shared state with the latest and cumulative results.
## The other thread runs the HTTP server, which responds to browser requests and generates the latest plot for the dashboard.


def consume():
    cum, cum_n = None, 0
    for msg in KafkaConsumer('topic_results', bootstrap_servers=BOOTSTRAP,
                             auto_offset_reset='latest', value_deserializer=lambda v: json.loads(v.decode('utf-8'))):
        m = msg.value
        p, n = np.array(m['avg_power']), m['n_scans']
        cum = p * n if cum is None else cum + p * n
        cum_n += n
        state.update(f=np.array(m['freq_hz']), p=p, e=np.array(m['std_power']),
                     cum=cum / cum_n, n=n, cum_n=cum_n, bid=m['batch_id'])


def render():
    s = state
    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.fill_between(s['f'], s['p'] - s['e'], s['p'] + s['e'], alpha=.25)
    ax.plot(s['f'], s['p'], lw=.8, label='this batch')
    ax.plot(s['f'], s['cum'], lw=1.3, label='cumulative')
    ax.set(xlabel='frequency [Hz]', ylabel='power', yscale='log',
           title=f"batch {s['bid']} — {s['n']} scans — {s['cum_n']} total")
    ax.legend()
    buf = io.BytesIO(); fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    return buf.getvalue()


PAGE = b"""<title>QUAX live</title>
<body style="margin:0;display:grid;place-items:center;height:100vh;font-family:sans-serif">
<img id=p style="max-width:100%">
<script>setInterval(()=>p.src='/plot.png?'+Date.now(),2000);p.src='/plot.png'</script>"""


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/plot.png'):
            body, ctype = (render(), 'image/png') if state else (b'', 'image/png')
        else:
            body, ctype = PAGE, 'text/html'
        self.send_response(200)
        self.send_header('Content-Type', ctype)
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a): pass


threading.Thread(target=consume, daemon=True).start()
print(f'http://0.0.0.0:{PORT}', flush=True)
HTTPServer(('0.0.0.0', PORT), H).serve_forever()
