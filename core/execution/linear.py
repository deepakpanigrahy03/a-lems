#!/usr/bin/env python3
"""
================================================================================
LINEAR AI EXECUTOR – Single LLM call, no tools
================================================================================

Purpose: Implements linear AI workflows as the baseline for measuring 
         orchestration tax. Single LLM call with no tool usage.

Why this exists:
    Linear AI is the CONTROL case for experiments. By comparing its energy
    consumption against agentic AI, we can quantify the "orchestration tax" –
    the additional energy overhead of planning, tool use, and synthesis.

SCIENTIFIC NOTES:
    - This executor uses a STANDARDIZED prompt format identical to agentic's
      base prompt to ensure fair comparison.
    - All timestamps are recorded for precise energy alignment.
    - Both cloud (Groq) and local (Ollama) providers are supported.

Requirements:
    Req 3.1: Dual-Harness Support – local/cloud via config
    Req 3.6: Device Handoff Latency – exact start/end timestamps

Author: Deepak Panigrahy
================================================================================
"""

import os
import time
import uuid
import psutil
import socket
import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime

from core.utils.debug import dprint

logger = logging.getLogger(__name__)


# ============================================================================
# STANDARDIZED PROMPT for fair comparison with agentic
# Same base prompt used by both executors
# ============================================================================
BASE_TASK_PROMPT = """
Task: {task}

Please provide a complete and thorough answer.
"""


class LinearExecutor:
    """
    Executes a single LLM call (linear workflow) with no tools.
    
    This is the baseline case for orchestration tax experiments:
    - One system prompt
    - One user message
    - One synchronous API call
    - Direct answer – no tool calls, no loops, no agent reasoning
    
    Its energy profile is the reference against which agentic overhead is measured.
    All configuration comes from Module 0 – no hardcoding.
    Debug output controlled by A_LEMS_DEBUG environment variable.
    """

    def __init__(self, model_config: Dict[str, Any]):
        """
        Initialize linear executor with model configuration from Module 0.
        
        Purpose:
            Load all settings from config files so the executor can work with
            different models (local/cloud) without code changes.
            
        Why this exists:
            Req 3.1 requires supporting both local and cloud models.
            All configuration comes from Module 0's models.json.
            
        Args:
            model_config: Dictionary containing:
                - provider: "groq", "anthropic", "openai", "ollama", etc.
                - api_endpoint: URL for API calls
                - api_key_env: Environment variable name for API key (cloud only)
                - model_id: Model identifier for the provider
                - max_tokens: Maximum tokens in response
                - temperature: Sampling temperature (0.0-1.0)
        """
        self.config = model_config
        self.api_key = os.getenv(self.config.get('api_key_env')) if self.config.get('api_key_env') else None
        self.max_tokens = self.config.get('max_tokens', 1024)
        self.temperature = self.config.get('temperature', 0.7)
        self.provider = self.config.get('provider', 'unknown')
        self.model_path = self.config.get('model_path') 
        
        if self.provider not in ['ollama', 'local'] and not self.api_key:
            logger.warning(f"API key missing: {self.config.get('api_key_env')}")
        logger.info(f"LinearExecutor initialized: {self.config.get('model_id')} ({self.provider})")

    def execute(self, prompt: str, temperature: Optional[float] = None) -> Dict[str, Any]:
        """
        Execute a single LLM call (linear workflow) with precise timing.
        
        Purpose:
            This is the baseline measurement for orchestration tax experiments.
            A single API call with no tools, planning, or synthesis.
            
        Why this exists:
            - Measures energy of pure LLM inference (Req 3.6)
            - Provides baseline for comparing agentic overhead
            - Tracks token usage for cost analysis
            - Exact start/end timestamps for energy correlation
            
        Args:
            prompt: User query or task description
            temperature: Optional override (0.0 for reproducibility, 0.7 for normal)
            
        Returns:
            Dictionary with:
                - experiment_id: Unique ID for traceability
                - start_time: Unix timestamp (for energy alignment)
                - end_time: Unix timestamp (for energy alignment)
                - response: Model output text
                - tokens: Token usage statistics
                - execution_time_ms: Total time (Req 3.6)
                - prompt_chars: Length of input
                - response_chars: Length of output
                - prompt_bytes: Size in bytes
                - response_bytes: Size in bytes
                - timestamp: ISO format timestamp
                - model: Model ID used
                - provider: Provider name
        """
        experiment_id = str(uuid.uuid4())[:8]
        start_time = time.time()
        # Capture network metrics before API call
        net_before = self._get_network_metrics()        
        dprint(f"\n{'='*60}")
        dprint(f"🚀 LINEAR EXECUTION [{experiment_id}]: {prompt[:100]}...")
        dprint(f"{'='*60}")
        
        if self.provider not in ['ollama', 'local'] and not self.api_key:
            logger.error("No API key available")
            return {
                "experiment_id": experiment_id,
                "start_time": start_time,
                "end_time": time.time(),
                "response": "Error: No API key",
                "tokens": {},
                "error": "API key not found"
            }
        
        # Use provided temperature or default
        temp = temperature if temperature is not None else self.temperature
        
        dprint(f"📨 Calling {self.provider} API (temp={temp})...")
        
        try:
            effective_kbps = 0
            api_latency_ms = 0
            tokens = {}
            content = ""
            response_bytes = 0
            prompt_bytes = 0

            # ====================================================================
            # Handle different provider formats
            # ====================================================================
            if self.provider == 'ollama':
                # Local Ollama API (no API key needed)
                api_start = time.time()
                # Calculate bytes sent
                prompt_bytes = len(prompt.encode('utf-8'))                
                response = requests.post(
                    self.config['api_endpoint'],
                    json={
                        "model": self.config['model_id'],
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False,
                        "options": {
                            "temperature": temp,
                            "num_predict": self.max_tokens
                        }
                    },
                    timeout=30
                )
                api_latency_ms = (time.time() - api_start) * 1000 
                response.raise_for_status()
                data = response.json()
                content = data['message']['content']
                response_bytes = len(content.encode('utf-8'))
                total_bytes = prompt_bytes + response_bytes
                effective_kbps = (total_bytes * 8) / (api_latency_ms / 1000) / 1000 if api_latency_ms > 0 else 0 
                # Track effective_kbps for this call
                if not hasattr(self, '_effective_kbps_list'):
                    self._effective_kbps_list = []
                self._effective_kbps_list.append(effective_kbps)  

                
                               
                # Ollama doesn't return tokens, estimate
                tokens = {
                    'prompt': len(prompt.split()),
                    'completion': len(content.split()),
                    'total': len(prompt.split()) + len(content.split())
                }
            # ====================================================================
            # NEW: Local GGUF model using llama-cpp-python
            # ====================================================================
            elif self.provider == 'local':
                api_start = time.time()
                prompt_bytes = len(prompt.encode('utf-8'))
                
                try:
                    from llama_cpp import Llama
                    
                    # Load model (consider caching for performance)
                    llm = Llama(model_path=self.model_path)
                    
                    # Run inference
                    response = llm(
                        prompt,
                        max_tokens=self.max_tokens,
                        temperature=temp,
                        echo=False
                    )
                    
                    api_latency_ms = (time.time() - api_start) * 1000
                    content = response['choices'][0]['text'].strip()
                    
                    # Get token counts from response
                    tokens = {
                        'prompt': response['usage']['prompt_tokens'],
                        'completion': response['usage']['completion_tokens'],
                        'total': response['usage']['total_tokens']
                    }
                    
                    # Calculate bytes for throughput (optional)
                    response_bytes = len(content.encode('utf-8'))
                    total_bytes = prompt_bytes + response_bytes
                    effective_kbps = (total_bytes * 8) / (api_latency_ms / 1000) / 1000 if api_latency_ms > 0 else 0
                    
                except Exception as e:
                    logger.error(f"Local model inference failed: {e}")
                    raise

            else:
                # Cloud APIs (Groq, OpenAI, etc.)
                headers = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                }
                # Calculate bytes sent

                prompt_bytes = len(prompt.encode('utf-8'))
                payload = {
                    "model": self.config['model_id'],
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": self.max_tokens,
                    "temperature": temp
                }
                api_start = time.time()
                response = requests.post(
                    self.config['api_endpoint'],
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                api_latency_ms = (time.time() - api_start) * 1000 
                response.raise_for_status()
                data = response.json()
                
                if 'choices' in data:
                    content = data['choices'][0]['message']['content']
                    usage = data.get('usage', {})
                    tokens = {
                        'prompt': usage.get('prompt_tokens', 0),
                        'completion': usage.get('completion_tokens', 0),
                        'total': usage.get('total_tokens', 0)
                    }
                    # Calculate bytes received and throughput
                    response_bytes = len(content.encode('utf-8'))
                    total_bytes = prompt_bytes + response_bytes
                    effective_kbps = (total_bytes * 8) / (api_latency_ms / 1000) / 1000 if api_latency_ms > 0 else 0

                else:
                    content = str(data)
                    tokens = {}
                    logger.warning(f"Unexpected API response format")
            
            end_time = time.time()
            execution_time_ms = (end_time - start_time) * 1000

            avg_effective_kbps = 0
            if hasattr(self, '_effective_kbps_list') and self._effective_kbps_list:
                avg_effective_kbps = sum(self._effective_kbps_list) / len(self._effective_kbps_list)
            # Capture network metrics after API call
            net_after = self._get_network_metrics()
            
            # Calculate network deltas
            bytes_sent = net_after['bytes_sent'] - net_before['bytes_sent']
            bytes_recv = net_after['bytes_recv'] - net_before['bytes_recv']
            tcp_retransmits = net_after['tcp_retransmits'] - net_before['tcp_retransmits']            
            result = {
                "experiment_id": experiment_id,
                "start_time": start_time,
                "end_time": end_time,
                "response": content,
                "tokens": tokens,
                "execution_time_ms": execution_time_ms,
                "api_latency_ms": api_latency_ms,
                "compute_time_ms": execution_time_ms,
                "effective_kbps": effective_kbps,
                "avg_effective_kbps": avg_effective_kbps,
                # NEW: Network metrics
                "bytes_sent": bytes_sent,
                "bytes_recv": bytes_recv,
                "tcp_retransmits": tcp_retransmits,                 
                "prompt_chars": len(prompt),
                "response_chars": len(content),
                "prompt_bytes": len(prompt.encode('utf-8')),
                "response_bytes": len(content.encode('utf-8')),
                "timestamp": datetime.now().isoformat(),
                "model": self.config.get('model_id'),
                "provider": self.provider
            }
            
            dprint(f"✅ Linear complete: {execution_time_ms:.0f}ms, {tokens.get('total', 0)} tokens")
            return result
            
        except Exception as e:
            end_time = time.time()
            logger.error(f"LLM call failed: {e}")
            return {
                "experiment_id": experiment_id,
                "start_time": start_time,
                "end_time": end_time,
                "response": f"Error: {e}",
                "tokens": {},
                "error": str(e),
                "execution_time_ms": (end_time - start_time) * 1000,
                "bytes_sent": 0,
                "bytes_recv": 0,
                "tcp_retransmits": 0                
                
            }

    def execute_comparison(self, task: str) -> Dict[str, Any]:
        """
        Execute with standardized prompt for fair comparison with agentic.
        
        This ensures linear and agentic see semantically equivalent tasks,
        removing bias from prompt engineering.
        
        Args:
            task: The task to solve
            
        Returns:
            Same as execute() but with standardized prompt
        """
        prompt = BASE_TASK_PROMPT.format(task=task)
        return self.execute(prompt)
    def _get_network_metrics(self) -> Dict[str, Any]:
        """
        Get network I/O metrics before/after API call.
        
        Returns:
            Dictionary with:
            - bytes_sent: Total bytes sent
            - bytes_recv: Total bytes received
            - tcp_retransmits: Number of TCP retransmissions
        """
        metrics = {}
        
        try:
            # Get network I/O counters
            net_io = psutil.net_io_counters()
            metrics['bytes_sent'] = net_io.bytes_sent
            metrics['bytes_recv'] = net_io.bytes_recv
            
            # Get TCP retransmits (Linux only)
            with open('/proc/net/snmp', 'r') as f:
                for line in f:
                    if line.startswith('Tcp:'):
                        parts = line.split()
                        if 'RetransSegs' in parts:
                            idx = parts.index('RetransSegs')
                            metrics['tcp_retransmits'] = int(parts[idx + 1])
                        break
        except Exception as e:
            logger.debug(f"Could not get network metrics: {e}")
            metrics['bytes_sent'] = 0
            metrics['bytes_recv'] = 0
            metrics['tcp_retransmits'] = 0
        
        return metrics    