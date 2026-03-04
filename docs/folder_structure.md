core/
├── __init__.py
├── energy_engine.py           # Main orchestrator
├── readers/
│   ├── __init__.py
│   ├── rapl_reader.py         # Req 1.1, 1.3, 1.46
│   ├── perf_reader.py         # Req 1.5, 1.6, 1.10, 1.43
│   ├── turbostat_reader.py    # Req 1.4, 1.7, 1.8, 1.41
│   ├── sensor_reader.py       # Req 1.2, 1.9, 1.38
│   ├── msr_reader.py          # Req 1.21, 1.27, 1.47
│   └── scheduler_monitor.py   # Req 1.12, 1.23, 1.24, 1.36
├── models/
│   ├── __init__.py
│   ├── energy_measurement.py  # Data classes
│   └── performance_counters.py # Data classes
├── utils/
│   ├── __init__.py
│   ├── core_pinner.py         # Req 1.15
│   ├── sampling.py            # Req 1.46
│   └── validators.py          # Measurement validation
└── exceptions/
    ├── __init__.py
    └── energy_exceptions.py   # Custom exceptions