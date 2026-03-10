---
title: A-LEMS Energy Measurement Dashboard
emoji: ⚡
colorFrom: blue
colorTo: green
sdk: streamlit
sdk_version: 1.32.0
app_file: streamlit_app.py
pinned: true
license: mit
---

# ⚡ A-LEMS — Agentic LLM Energy Measurement System

**PhD Research Dashboard** — Measuring the orchestration energy tax of agentic
vs linear LLM execution using Intel RAPL hardware counters.

---

## Two Modes

**⚫ Offline (always on)** — The full analysis dashboard runs 24/7 directly
from this Space. All 18 pages work: energy breakdowns, sustainability metrics,
SQL lab, research insights, anomaly detection, and more. Reads from the
`data/experiments.db` file in this repo.

**🟢 Live (when the lab is online)** — When the lab owner runs
`tunnel_agent.py` on their measurement laptop, researchers can connect via the
sidebar, trigger real experiments remotely, and watch live telemetry stream
back in real time.

---

## For Researchers

Open this Space. Everything works immediately.

To trigger a **live experiment**, click **⚡ Connect to Live Lab** in the
sidebar and enter the tunnel URL and token shared by the lab owner.
The URL is permanent — bookmark it once and it works every time the lab is
online.

---

## For the Lab Owner

### One-time setup (~5 min)

**1. Clone this Space and set up Git LFS**

```bash
git clone https://huggingface.co/spaces/YOUR_USERNAME/a-lems
cd a-lems
git lfs install
pip install -r requirements.txt pyyaml uvicorn fastapi
```

**2. Install cloudflared**

```bash
wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb
sudo dpkg -i cloudflared-linux-amd64.deb
```

**3. Create your permanent tunnel (one time only)**

```bash
cloudflared tunnel login          # opens browser → click Authorise
cloudflared tunnel create a-lems  # prints a UUID — copy it
```

**4. Configure `config/tunnel.yaml`**

```yaml
tunnel_name: "a-lems"
tunnel_uuid: "paste-your-UUID-here"
token: "alems-choose-a-passphrase"   # researchers enter this in the sidebar
custom_hostname: ""                  # optional: map to your own domain
```

Generate a token:
```bash
python -c "import secrets; print('alems-' + secrets.token_urlsafe(12))"
```

**5. Go live**

```bash
source venv/bin/activate
python tunnel_agent.py
```

Output:
```
╔══════════════════════════════════════════════════════════════╗
║  🟢  A-LEMS is LIVE                                         ║
║                                                              ║
║  Permanent URL  →  https://xxxxxxxx-xxxx.cfargotunnel.com  ║
║  Token          →  alems-yourpassphrase                     ║
║                                                              ║
║  Share these once by email. URL never changes.              ║
╚══════════════════════════════════════════════════════════════╝
```

Share the URL and token once by email. Done forever.

**Run in background (survives terminal close):**
```bash
nohup python tunnel_agent.py > logs/tunnel.log 2>&1 &
tail -f logs/tunnel.log
```

### Update the database after experiments

```bash
# On your laptop after running experiments:
git add data/experiments.db
git commit -m "db: +24 runs — factual_qa, reasoning tasks"
git push
# HF Space updates automatically within ~60 seconds
```

---

## Architecture

```
Hugging Face Space  (always on, free, zero branding)
  https://YOUR_USERNAME-a-lems.hf.space
  │
  ├─ OFFLINE: reads experiments.db from this repo
  │   All 18 analysis pages work 24/7
  │
  └─ ONLINE: researcher connects via sidebar
      URL + token → tunnels to your laptop
      Execute Run → runs on your RAPL-equipped machine
      Live telemetry → streams back to their browser

Your Laptop  (when tunnel_agent.py is running)
  cloudflared named tunnel → permanent cfargotunnel.com URL
  FastAPI server.py on :8765
  Measurement harness (RAPL counters)
```

---

## Project Structure

```
a-lems/
├── streamlit_app.py        # Entry point
├── tunnel_agent.py         # Run this to go live
├── server.py               # FastAPI backend
├── requirements.txt
├── config/
│   ├── tunnel.yaml         # Cloudflare + token config ← edit this
│   └── research_insights.yaml
├── data/
│   └── experiments.db      # SQLite (tracked via Git LFS)
└── gui/
    ├── connection.py       # Tunnel connection manager
    ├── sidebar.py          # Nav + Live Lab panel
    ├── config.py / db.py / helpers.py
    └── pages/              # 18 analysis pages
```

---

## Security

| What | How |
|------|-----|
| View data | Public — anyone with the Space URL |
| Trigger experiments | Requires shared token |
| Revoke access | Change `token` in `tunnel.yaml`, restart `tunnel_agent.py` |
| Credentials | `.cloudflared/*.json` and `.env` are in `.gitignore` — never committed |

---

## Citation

```bibtex
@misc{alems2026,
  title  = {A-LEMS: Agentic LLM Energy Measurement System},
  author = {Your Name},
  year   = {2026},
  url    = {https://huggingface.co/spaces/YOUR_USERNAME/a-lems}
}
```
