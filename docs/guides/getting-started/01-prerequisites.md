# Prerequisites

## System Requirements
- **OS:** Ubuntu 22.04+ / Debian 11+
- **CPU:** Intel 6th gen+ (for RAPL support)
- **RAM:** 8GB minimum (16GB recommended)
- **Storage:** 10GB free space
- **Python:** 3.10+

## Install System Dependencies
```bash
sudo apt update
sudo apt install -y python3-pip python3-venv git build-essential \
    linux-tools-common linux-tools-generic msr-tools lm-sensors
