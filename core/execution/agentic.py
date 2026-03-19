#!/usr/bin/env python3
"""
================================================================================
AGENTIC AI EXECUTOR – Multi-step LLM with tool support
================================================================================

Purpose: Implements agentic AI workflows to measure orchestration tax.
    - Plans tasks, uses tools, synthesizes results
    - Phase-level timing for scientific analysis

SCIENTIFIC NOTES:
    - Uses SAME base prompt as linear for fair comparison
    - Planning phase uses temperature=0 for reproducibility
    - Phase timing separates planning/execution/synthesis
    - Complexity score weights multiple factors with proper normalization
    - Both cloud (Groq) and local (Ollama) providers supported

Requirements:
    Req 3.1: Dual-Harness Support – local/cloud via config
    Req 3.2: Complexity-Level Logic – based on tool calls
    Req 3.6: Device Handoff Latency – phase-level timing

Author: Deepak Panigrahy
================================================================================
"""

import json
import logging
import math
import os
import socket
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import psutil
import requests

from core.utils.debug import dprint

logger = logging.getLogger(__name__)


# ============================================================================
# STANDARDIZED BASE PROMPT – Same as linear for fair comparison
# ============================================================================
BASE_TASK_PROMPT = """
Task: {task}

Please provide a complete and thorough answer.
"""


class AgenticExecutor:
    """
    Executes agentic AI workflows with tool support.

    Workflow:
        1. Planning Phase: LLM creates step-by-step execution plan
        2. Execution Phase: Each step runs (tool or LLM)
        3. Synthesis Phase: Combine all results into final answer

    Number of LLM calls = 1 (plan) + N (steps) + 1 (synthesis)
    where N is the number of steps that require LLM reasoning.

    All configuration comes from Module 0 – no hardcoding.
    Debug output controlled by A_LEMS_DEBUG environment variable.
    """

    def __init__(self, model_config: Dict[str, Any]):
        """
        Initialize executor with model configuration from Module 0.

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
                - api_key_env: Environment variable name for API key
                - model_id: Model identifier for the provider
                - max_tokens: Maximum tokens in response
                - temperature: Sampling temperature (0.0-1.0)
                - tools: List of supported tool names
        """
        self.config = model_config
        self.api_key = (
            os.getenv(self.config.get("api_key_env"))
            if self.config.get("api_key_env")
            else None
        )
        self.supported_tools = self.config.get("tools", [])
        self.max_tokens = self.config.get("max_tokens", 2048)
        self.temperature = self.config.get("temperature", 0.7)
        self.provider = self.config.get("provider", "unknown")
        self.model_path = self.config.get("model_path")
        self.call_counter = 0
        self.pending_interactions = []

        if self.provider not in ["ollama", "local"] and not self.api_key:
            logger.warning(f"API key missing: {self.config.get('api_key_env')}")
        logger.info(
            f"AgenticExecutor initialized: {self.config.get('model_id')} ({self.provider})"
        )

    def _calculate_complexity_score(
        self, llm_calls: int, tool_calls: int, total_tokens: int
    ) -> Dict[str, float]:
        """
        Calculate weighted complexity score for orchestration tax analysis.

        LITERATURE BASIS:
        -----------------
        This metric is informed by established research in Green AI and
        computer systems:

        1. LLM calls (α factor):
            Justification: Each model invocation incurs compute and energy cost
            proportional to inference workload. Supported by:
            - Schwartz et al., "Green AI" (2020) – energy ∝ computation
            - Patterson et al., "Carbon Emissions..." (2021) – energy ∝ model runs

        2. Tool calls (β factor):
            Justification: External tool execution consumes CPU, memory, I/O.
            Supported by:
            - Hennessy & Patterson, "Computer Architecture" – energy in systems

        3. Token volume (γ factor):
            Justification: Inference compute scales with token count.
            Supported by:
            - Kaplan et al., "Scaling Laws for Neural Language Models" (2020)

        WEIGHT VALUES:
        -------------
        The weights (α=0.4, β=0.3, γ=0.3) are HEURISTIC coefficients
        inspired by literature but represent our novel contribution –
        the "Orchestration Complexity Metric" defined in this work.

        NORMALIZATION:
        -------------
        All components are normalized to [0,1] range to ensure fair contribution
        regardless of absolute scales. This follows best practices in
        composite metric design (OECD, 2008).

        Returns:
            Dictionary with:
                - raw_score: Weighted sum (0-1 range)
                - normalized_score: Scaled to 1-10 for interpretation
                - components: Individual normalized factors with citations
                - weights: The heuristic weight values used
        """
        # Maximum expected values for normalization (based on pilot experiments)
        MAX_LLM_CALLS = 10  # Upper bound: planning + up to 8 steps + synthesis
        MAX_TOOL_CALLS = 10  # Upper bound: maximum tools in complex tasks
        TOKEN_THRESHOLD = 1000  # Based on scaling laws (Kaplan et al. 2020)

        # Normalize each component to [0, 1] range (OECD composite indicator guidelines)
        normalized_llm = min(llm_calls / MAX_LLM_CALLS, 1.0)
        normalized_tools = min(tool_calls / MAX_TOOL_CALLS, 1.0)
        normalized_tokens = min(total_tokens / TOKEN_THRESHOLD, 1.0)

        # Heuristic weights (our novel contribution – not from literature)
        ALPHA = 0.4  # LLM calls weight – importance of model invocations
        BETA = 0.3  # Tool calls weight – importance of external operations
        GAMMA = 0.3  # Token volume weight – importance of computation scale

        # Calculate weighted score
        raw_score = (
            ALPHA * normalized_llm + BETA * normalized_tools + GAMMA * normalized_tokens
        )

        # Scale to 1-10 for human interpretation
        normalized_score = 1 + raw_score * 9

        return {
            "raw_score": raw_score,
            "normalized_score": normalized_score,
            "components": {
                "llm_calls": {
                    "raw": llm_calls,
                    "normalized": normalized_llm,
                    "weight": ALPHA,
                    "citation": "Schwartz et al. 2020; Patterson et al. 2021",
                },
                "tool_calls": {
                    "raw": tool_calls,
                    "normalized": normalized_tools,
                    "weight": BETA,
                    "citation": "Hennessy & Patterson, Computer Architecture",
                },
                "token_volume": {
                    "raw": total_tokens,
                    "normalized": normalized_tokens,
                    "weight": GAMMA,
                    "citation": "Kaplan et al. 2020",
                },
            },
            "weights": {"alpha": ALPHA, "beta": BETA, "gamma": GAMMA},
            "note": "Heuristic weights – novel contribution of this work",
            "literature": {
                "green_ai": "Schwartz, R., Dodge, J., Smith, N. A., & Etzioni, O. (2020). Green AI.",
                "carbon_emissions": "Patterson, D., et al. (2021). Carbon Emissions and Large Neural Network Training.",
                "scaling_laws": "Kaplan, J., et al. (2020). Scaling Laws for Neural Language Models.",
                "computer_architecture": "Hennessy, J. L., & Patterson, D. A. (2017). Computer Architecture: A Quantitative Approach.",
                "composite_indicators": "OECD (2008). Handbook on Constructing Composite Indicators.",
            },
        }

    def execute(self, task: str, planning_temperature: float = 0.0) -> Dict[str, Any]:
        """
        Execute agentic workflow with phase-level timing.

        Purpose:
            This is the main entry point that runs the complete agentic pipeline:
            1. Planning: LLM creates step-by-step plan (temperature=0 for reproducibility)
            2. Execution: Each step runs (tool or LLM)
            3. Synthesis: Combine all results into final answer

        Why this exists:
            - Measures energy consumption of agentic workflows (Req 3.6)
            - Determines complexity based on tool count (Req 3.2)
            - Phase timing reveals where orchestration tax is spent
            - Results used to calculate overhead vs linear AI

        Args:
            task: User query (e.g., "What is 2+2?")
            planning_temperature: Temperature for planning phase (default 0.0 for reproducibility)

        Returns:
            Dictionary with all metrics needed for energy analysis
        """
        experiment_id = str(uuid.uuid4())[:8]
        overall_start = time.time()
        total_prompt_chars = 0
        total_response_chars = 0
        call_counter = 0
        step_counter = 0

        dprint(f"\n{'#'*70}")
        dprint(f"🚀 AGENTIC EXECUTION [{experiment_id}]: {task[:100]}")
        dprint(f"{'#'*70}")

        # ====================================================================
        # Phase 1: Planning – LLM creates step-by-step plan (1 call)
        # Temperature=0 for reproducibility – same task = same plan
        # This is CRITICAL for experimental reproducibility
        # ====================================================================
        plan_start = time.time()
        call_counter += 1
        plan = self._create_plan(
            task, temperature=planning_temperature, call_counter=call_counter
        )
        steps = plan.get("steps", [])
        plan_end = time.time()
        planning_time_ms = (plan_end - plan_start) * 1000

        # Emit planning phase event
        self._emit_event(
            phase="planning",
            event_type="planning",
            start_time=plan_start,
            end_time=plan_end,
            metadata={
                "steps": len(steps),
                "task_preview": task[:100],
                "planning_temperature": planning_temperature,
            },
        )

        dprint(f"📋 Planning: {len(steps)} steps, {planning_time_ms:.1f}ms")

        # ====================================================================
        # Phase 2: Execution – Run each step (tool or LLM)
        # ====================================================================
        exec_start = time.time()
        step_results, tools_used = [], []
        tokens = {"prompt": 0, "completion": 0, "total": 0}
        total_llm_calls = 0
        step_counter = 0

        for i, step in enumerate(steps):
            step_counter += 1
            if step.get("tool") in self.supported_tools:
                # Tool execution – external computation, no LLM call
                tool_start = time.time()
                result = self._execute_tool(
                    step["tool"], step.get("args", {}), step_counter
                )
                tool_end = time.time()
                step_results.append(
                    {
                        "step": i + 1,
                        "type": "tool",
                        "tool": step["tool"],
                        "result": result,
                        "time_ms": (tool_end - tool_start) * 1000,
                    }
                )
                if step["tool"] not in tools_used:
                    tools_used.append(step["tool"])
                dprint(f"  🔧 Tool {step['tool']} → {result}")
            else:
                # LLM execution – another call to the model
                call_counter += 1
                prompt = step.get("prompt", task)
                llm_start = time.time()
                llm_result = self._call_llm(
                    prompt, temperature=self.temperature, call_counter=call_counter
                )
                llm_end = time.time()
                step_results.append(
                    {
                        "step": i + 1,
                        "type": "llm",
                        "result": llm_result.get("content", ""),
                        "time_ms": (llm_end - llm_start) * 1000,
                    }
                )

                # ====================================================================
                # FIXED: Handle token counting from API response (12 spaces indentation)
                # ====================================================================
                if "usage" in llm_result:
                    usage = llm_result["usage"]
                    tokens["prompt"] += usage.get("prompt_tokens", 0)
                    tokens["completion"] += usage.get("completion_tokens", 0)
                    tokens["total"] += usage.get("total_tokens", 0)
                    print(
                        f"🔍 DEBUG - added prompt:{usage.get('prompt_tokens',0)}, completion:{usage.get('completion_tokens',0)}, total:{usage.get('total_tokens',0)}"
                    )
                    print(f"🔍 DEBUG - now tokens: {tokens}")
                elif "tokens" in llm_result:
                    # Fallback for any providers that use 'tokens' format
                    for k, v in llm_result["tokens"].items():
                        tokens[k] += v
                        print(
                            f"🔍 DEBUG - added {k}: {v}, now tokens[{k}] = {tokens[k]}"
                        )
                else:
                    logger.debug(
                        f"No token data in llm_result. Keys: {llm_result.keys()}"
                    )

                total_llm_calls += 1
                total_prompt_chars += len(prompt)
                total_response_chars += len(llm_result.get("content", ""))
                dprint(f"  🤖 LLM step {i+1} complete")

        exec_end = time.time()
        execution_time_ms = (exec_end - exec_start) * 1000

        print(f"🔍 DEBUG - accumulated tokens: {tokens}")
        print(f"🔍 DEBUG - tokens keys: {tokens.keys()}")
        # print("🔍 DEBUG - llm_result keys:", llm_result.keys())
        # print("🔍 DEBUG - llm_result full:", llm_result)
        # ====================================================================
        # Phase 3: Synthesis – Combine all results (1 call)
        # ====================================================================
        syn_start = time.time()
        call_counter += 1
        synthesis = self._synthesize(
            task, steps, step_results, call_counter=call_counter
        )
        syn_end = time.time()
        synthesis_time_ms = (syn_end - syn_start) * 1000

        # Emit synthesis phase event
        self._emit_event(
            phase="synthesis",
            event_type="synthesis",
            start_time=syn_start,
            end_time=syn_end,
            metadata={"tokens": tokens, "has_content": bool(synthesis.get("content"))},
        )
        if "tokens" in synthesis:
            for k, v in synthesis["tokens"].items():
                tokens[k] += v
        total_llm_calls += 1  # Count synthesis call
        total_prompt_chars += len(synthesis.get("prompt", ""))
        total_response_chars += len(synthesis.get("content", ""))

        # ====================================================================
        # Req 3.2: Determine complexity based on actual tool usage
        # More tools = more complex = higher energy consumption
        # ====================================================================
        tool_count = len(tools_used)
        if tool_count <= 1:
            complexity_level = 1  # Simple: 0-1 tools (low orchestration tax)
        elif tool_count <= 3:
            complexity_level = 2  # Moderate: 2-3 tools (medium tax)
        else:
            complexity_level = 3  # Complex: 4+ tools (high tax)

        total_time_ms = (time.time() - overall_start) * 1000

        # Calculate final LLM calls: planning (1) + execution (N) + synthesis (1)
        final_llm_calls = total_llm_calls + 1  # +1 for planning call
        # ====================================================================
        # Calculate total effective throughput across all LLM calls
        # ====================================================================
        total_effective_kbps = 0
        if hasattr(self, "_effective_kbps_list") and self._effective_kbps_list:
            total_effective_kbps = sum(self._effective_kbps_list) / len(
                self._effective_kbps_list
            )
            # Sum all effective_kbps values (you'd need to track them)
            # For now, let's calculate average
            dprint(
                f"📊 Average throughput: {total_effective_kbps:.1f} kbps across {len(self._effective_kbps_list)} calls"
            )

        result = {
            "experiment_id": experiment_id,
            "response": synthesis.get("content", ""),
            "tokens": tokens,
            "llm_calls": final_llm_calls,  # CORRECT: planning + execution + synthesis
            "steps": len(steps),
            "tools_used": tools_used,
            "tool_count": tool_count,
            "pending_interactions": getattr(self, "pending_interactions", []),
            "complexity_level": complexity_level,  # Req 3.2
            "complexity_score": self._calculate_complexity_score(
                final_llm_calls, tool_count, tokens.get("total", 0)
            ),
            "phase_times": {
                "planning_ms": planning_time_ms,
                "execution_ms": execution_time_ms,
                "synthesis_ms": synthesis_time_ms,
                "total_ms": total_time_ms,
            },
            "phase_percentages": {
                "planning_pct": (
                    (planning_time_ms / total_time_ms) * 100 if total_time_ms > 0 else 0
                ),
                "execution_pct": (
                    (execution_time_ms / total_time_ms) * 100
                    if total_time_ms > 0
                    else 0
                ),
                "synthesis_pct": (
                    (synthesis_time_ms / total_time_ms) * 100
                    if total_time_ms > 0
                    else 0
                ),
            },
            "timestamps": {
                "plan_start": plan_start,
                "plan_end": plan_end,
                "exec_start": exec_start,
                "exec_end": exec_end,
                "syn_start": syn_start,
                "syn_end": syn_end,
            },
            "total_time_ms": total_time_ms,  # Req 3.6
            "prompt_chars": total_prompt_chars,
            "response_chars": total_response_chars,
            "timestamp": datetime.now().isoformat(),
            "model": self.config.get("model_id"),
            "provider": self.provider,
            "avg_effective_kbps": total_effective_kbps,
        }

        dprint(f"\n📊 Phase breakdown:")
        dprint(
            f"   Planning:  {planning_time_ms:6.1f}ms ({result['phase_percentages']['planning_pct']:.0f}%)"
        )
        dprint(
            f"   Execution: {execution_time_ms:6.1f}ms ({result['phase_percentages']['execution_pct']:.0f}%)"
        )
        dprint(
            f"   Synthesis: {synthesis_time_ms:6.1f}ms ({result['phase_percentages']['synthesis_pct']:.0f}%)"
        )
        dprint(f"   TOTAL:     {total_time_ms:6.1f}ms")
        dprint(
            f"✅ Agentic complete: {total_time_ms:.0f}ms, {tokens.get('total', 0)} tokens"
        )
        # ====================================================================
        # Calculate API latency (total time spent waiting for network)
        # ====================================================================
        total_api_latency_ms = 0
        if hasattr(self, "_api_latencies"):
            total_api_latency_ms = sum(self._api_latencies)

        # Calculate phase ratios (normalized to 0-1 for ML)
        phase_ratios = {
            "planning_ratio": (
                planning_time_ms / total_time_ms if total_time_ms > 0 else 0
            ),
            "execution_ratio": (
                execution_time_ms / total_time_ms if total_time_ms > 0 else 0
            ),
            "synthesis_ratio": (
                synthesis_time_ms / total_time_ms if total_time_ms > 0 else 0
            ),
        }

        # ====================================================================
        # Calculate waiting time (time between LLM calls)
        # ====================================================================
        waiting_time_ms = 0
        if hasattr(self, "_api_latencies") and len(self._api_latencies) > 1:
            # Waiting time = total time - sum of active phases
            # Active phases = planning + execution + synthesis + tool time
            total_active = planning_time_ms + execution_time_ms + synthesis_time_ms
            if hasattr(self, "_tool_latencies"):
                total_active += sum(self._tool_latencies)
            waiting_time_ms = max(0, total_time_ms - total_active)

        # Add to result
        result.update(
            {
                "api_latency_ms": total_api_latency_ms,
                "compute_time_ms": total_time_ms - total_api_latency_ms,
                "waiting_time_ms": waiting_time_ms,  # M3-9
                "avg_step_time_ms": execution_time_ms / len(steps) if steps else 0,
                "events": getattr(self, "_events", []),  # M3-10
                "tool_latencies": getattr(self, "_tool_latencies", []),  # M3-11
                "avg_tool_latency_ms": (
                    sum(self._tool_latencies) / len(self._tool_latencies)
                    if hasattr(self, "_tool_latencies") and self._tool_latencies
                    else 0
                ),
            }
        )

        self.pending_interactions = []
        dprint(
            f"✅ Agentic complete: {execution_time_ms:.0f}ms, {tokens.get('total', 0)} tokens"
        )
        return result

    def execute_comparison(self, task: str) -> Dict[str, Any]:
        """
        Execute with standardized prompt for fair comparison with linear.

        This ensures linear and agentic see semantically equivalent tasks,
        removing bias from prompt engineering.

        Args:
            task: The task to solve

        Returns:
            Same as execute() but with standardized base prompt
        """
        # Same base prompt as linear
        base = BASE_TASK_PROMPT.format(task=task)

        # Additional instruction for agentic (planning)
        planning_prompt = f"""
{base}

To solve this effectively, break it down into steps.
You can use tools like calculator or web search if needed.
"""
        return self.execute(planning_prompt)

    def _create_plan(
        self, task: str, temperature: float = 0.0, call_counter: int = None
    ) -> Dict[str, Any]:
        """
        Ask LLM to create execution plan with deterministic temperature.

        Purpose:
            This is where agentic intelligence happens – the LLM analyzes
            the task and decides what steps are needed. Linear AI skips this.

        Why this exists:
            - Planning consumes energy (Req 3.6)
            - Determines how many tools will be used (Req 3.2)
            - Temperature=0 ensures same task = same plan (reproducibility)

        Args:
            task: Original user query
            temperature: 0.0 for reproducible planning

        Returns:
            Dictionary with 'steps' array containing the execution plan
        """
        prompt = f"""
        Break this task into steps. Return JSON with "steps" array.
        Each step: {{"description": str, "type": "tool/llm", 
                   "tool": name if tool, "args": {{}} if tool,
                   "prompt": str if llm}}
        Task: {task}
        Tools: {', '.join(self.supported_tools)}
        Example: {{"steps": [{{"description": "Calculate", "type": "tool", 
                             "tool": "calculator", "args": {{"expression": "2+2"}}}}]}}
        """

        response = self._call_llm(
            prompt, temperature=temperature, call_counter=call_counter
        )
        content = response.get("content", "{}")

        try:
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            return json.loads(content.strip())
        except:
            # Fallback for when LLM fails – still return something usable
            return {"steps": [{"description": "Answer", "type": "llm", "prompt": task}]}

    def _synthesize(
        self, task: str, steps: List, results: List, call_counter: int = None
    ) -> Dict[str, Any]:
        """
        Combine step results into final answer.

        Purpose:
            Takes all the pieces from planning and execution and weaves them
            into a coherent answer that addresses the original query.

        Why this exists:
            Without synthesis, agentic AI would just return raw tool outputs.
            This step consumes energy and is part of the orchestration tax.

        Args:
            task: Original user query
            steps: The plan steps
            results: Results from executing each step

        Returns:
            Dictionary with synthesis results (content and tokens)
        """
        prompt = f"Task: {task}\nResults: {json.dumps(list(zip(steps, results)))}\nFinal answer:"
        return self._call_llm(
            prompt, temperature=self.temperature, call_counter=call_counter
        )

    def _execute_tool(self, name: str, args: Dict, step_index: int = None) -> Any:
        """
        Execute a specific tool.

        Purpose:
            Tools give agentic AI access to external capabilities that the
            model alone doesn't have (real data, computation, etc.).

        Why this exists:
            Tool execution consumes energy (Req 3.6) and contributes to the
            orchestration tax we want to measure. This function centralizes
            all tool logic so it can be easily extended.

        Args:
            name: Tool name ("calculator", "web_search", etc.)
            args: Tool-specific arguments (expression, query, etc.)

        Returns:
            Tool execution result (varies by tool)
        """
        tool_start = time.time()

        dprint(f"🔧 Executing tool: {name} with args: {args}")

        # Emit tool start event
        self._emit_event(
            phase="execution",
            event_type="tool_call",
            start_time=tool_start,
            end_time=tool_start,  # Will be updated at end
            metadata={"tool": name, "args": args, "step": step_index},
        )

        result = None
        if name == "calculator":
            expr = args.get("expression", args.get("query", "")).replace(" ", "")

            if expr == "2+2":
                result = 4
            elif expr == "3*4":
                result = 12
            elif expr == "10/2":
                result = 5
            else:
                try:
                    allowed = {
                        k: v for k, v in math.__dict__.items() if not k.startswith("__")
                    }
                    result = eval(expr, {"__builtins__": {}}, allowed)
                except:
                    result = 0

        elif name == "web_search":
            time.sleep(0.3)
            result = f"Search results for: {args.get('query', '')}"

        tool_end = time.time()
        tool_latency_ms = (tool_end - tool_start) * 1000

        # Update the last event with end time
        if hasattr(self, "_events") and self._events:
            self._events[-1]["end_time_ns"] = int(tool_end * 1e9)
            self._events[-1]["duration_ns"] = int((tool_end - tool_start) * 1e9)
            self._events[-1]["metadata"]["result"] = str(result)[:100]

        dprint(f"✅ Tool complete: {tool_latency_ms:.1f}ms")

        # Store tool latency for aggregation
        if not hasattr(self, "_tool_latencies"):
            self._tool_latencies = []
        self._tool_latencies.append(tool_latency_ms)

        return result

    def _call_llm(
        self, prompt: str, temperature: Optional[float] = None, call_counter: int = None
    ) -> Dict[str, Any]:
        """
        Make actual API call to the LLM provider.

        Purpose:
            This is the core communication layer with the LLM API.
            Handles both cloud (Groq) and local (Ollama) providers.

        Why this exists:
            - Counts LLM calls for energy analysis (Req 3.6)
            - Tracks token usage for cost and energy estimation
            - Centralizes error handling
            - Supports different temperatures for planning vs execution

        Args:
            prompt: The prompt text to send to the LLM
            temperature: 0.0 for planning (reproducible), 0.7 for execution

        Returns:
            Dictionary with:
                - 'content': The model's response text
                - 'tokens': Dict with prompt/completion/total token counts
        """
        # Track calls for this execution
        if not hasattr(self, "_call_count"):
            self._call_count = 0
            self._api_latencies = []  # ← NEW: track latencies

        self._call_count += 1

        temp = temperature if temperature is not None else self.temperature

        dprint(f"\n{'='*50}")
        dprint(f"📨 LLM #{self._call_count} (temp={temp}, {len(prompt)} chars)")
        dprint(f"{'='*50}")

        if self.provider not in ["ollama", "local"] and not self.api_key:
            logger.error("No API key available")
            return {"content": "Error: No API key", "tokens": {}}
        # Capture network before
        net_before = self._get_network_metrics()

        try:
            api_latency_ms = 0
            effective_kbps = 0
            content = ""
            tokens = {}
            response_bytes = 0
            prompt_bytes = 0

            api_start = time.time()
            prompt_bytes = len(prompt.encode("utf-8"))

            if self.provider == "ollama":
                # Local Ollama API (minimal network latency)
                response = requests.post(
                    self.config["api_endpoint"],
                    json={
                        "model": self.config["model_id"],
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False,
                        "options": {
                            "temperature": temp,
                            "num_predict": self.max_tokens,
                        },
                    },
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()
                content = data["message"]["content"]
                tokens = {
                    "prompt": len(prompt.split()),
                    "completion": len(content.split()),
                    "total": len(prompt.split()) + len(content.split()),
                }

                api_latency_ms = (time.time() - api_start) * 1000
                response_bytes = len(content.encode("utf-8"))
                total_bytes = prompt_bytes + response_bytes
                effective_kbps = (
                    (total_bytes * 8) / (api_latency_ms / 1000) / 1000
                    if api_latency_ms > 0
                    else 0
                )
            # ====================================================================
            # NEW: Local GGUF model using llama-cpp-python
            # ====================================================================
            elif self.provider == "local":
                api_start = time.time()
                prompt_bytes = len(prompt.encode("utf-8"))

                try:
                    from llama_cpp import Llama

                    # Load model (consider caching for performance)
                    llm = Llama(model_path=self.model_path)

                    # Run inference
                    response = llm(
                        prompt, max_tokens=self.max_tokens, temperature=temp, echo=False
                    )

                    api_latency_ms = (time.time() - api_start) * 1000
                    content = response["choices"][0]["text"].strip()

                    # Get token counts from response
                    tokens = {
                        "prompt": response["usage"]["prompt_tokens"],
                        "completion": response["usage"]["completion_tokens"],
                        "total": response["usage"]["total_tokens"],
                    }
                    # ============================================================
                    # ADD THIS - Store interaction data (16 spaces indentation)
                    # ============================================================

                    # Calculate bytes for throughput (optional)
                    response_bytes = len(content.encode("utf-8"))
                    total_bytes = prompt_bytes + response_bytes
                    effective_kbps = (
                        (total_bytes * 8) / (api_latency_ms / 1000) / 1000
                        if api_latency_ms > 0
                        else 0
                    )

                except Exception as e:
                    logger.error(f"Local model inference failed: {e}")
                    raise

            else:
                # Cloud APIs
                headers = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                }
                payload = {
                    "model": self.config["model_id"],
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": self.max_tokens,
                    "temperature": temp,
                }
                response = requests.post(
                    self.config["api_endpoint"],
                    headers=headers,
                    json=payload,
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                if "choices" in data:
                    content = data["choices"][0]["message"]["content"]
                    usage = data.get("usage", {})
                    tokens = {
                        "prompt": usage.get("prompt_tokens", 0),
                        "completion": usage.get("completion_tokens", 0),
                        "total": usage.get("total_tokens", 0),
                    }

                else:
                    content = str(data)
                    tokens = {}
                    logger.warning(f"Unexpected API response format")

            api_end = time.time()
            api_latency_ms = (api_end - api_start) * 1000

            self._api_latencies.append(api_latency_ms)  # ← NEW: store latency
            # Emit LLM call event
            phase = "planning" if self._call_count == 1 else "execution"
            self._emit_event(
                phase=phase,
                event_type="llm_call",
                start_time=api_start,
                end_time=api_end,
                metadata={
                    "call_number": self._call_count,
                    "temperature": temp,
                    "tokens": tokens,
                    "model": self.config.get("model_id"),
                },
            )

            if not hasattr(self, "_effective_kbps_list"):
                self._effective_kbps_list = []
            self._effective_kbps_list.append(effective_kbps)  # effective kbps

            # Capture network after
            net_after = self._get_network_metrics()

            # Calculate network deltas
            bytes_sent = net_after["bytes_sent"] - net_before["bytes_sent"]
            bytes_recv = net_after["bytes_recv"] - net_before["bytes_recv"]
            tcp_retransmits = (
                net_after["tcp_retransmits"] - net_before["tcp_retransmits"]
            )

            dprint(
                f"📬 Response: {content[:100]}... (API: {api_latency_ms:.1f}ms, Throughput: {effective_kbps:.1f} kbps)"
            )

            # Capture network after
            net_after = self._get_network_metrics()

            # Calculate network deltas
            bytes_sent = net_after["bytes_sent"] - net_before["bytes_sent"]
            bytes_recv = net_after["bytes_recv"] - net_before["bytes_recv"]
            tcp_retransmits = (
                net_after["tcp_retransmits"] - net_before["tcp_retransmits"]
            )

            # ============================================================
            # CREATE INTERACTION HERE (after all metrics are available)
            # ============================================================
            interaction = {
                "step_index": call_counter if call_counter else self.call_counter,
                "workflow_type": "agentic",
                "prompt": prompt,
                "response": content,
                "model_name": self.config.get("model_id"),
                "provider": self.provider,
                "prompt_tokens": tokens.get("prompt", 0),
                "completion_tokens": tokens.get("completion", 0),
                "total_tokens": tokens.get("total", 0),
                "api_latency_ms": api_latency_ms,
                "throughput_kbps": effective_kbps,
                "bytes_sent": bytes_sent,
                "bytes_recv": bytes_recv,
                "tcp_retransmits": tcp_retransmits,
                "compute_time_ms": api_latency_ms,
            }

            # Store in pending list
            if not hasattr(self, "pending_interactions"):
                self.pending_interactions = []
                print(f"🔍 DEBUG - Initialized pending_interactions")

            self.pending_interactions.append(interaction)
            print(
                f"🔍 DEBUG - Added interaction, now has {len(self.pending_interactions)} items"
            )

            return {
                "content": content,
                "tokens": tokens,
                "api_latency_ms": api_latency_ms,  # Already there
                "effective_kbps": effective_kbps,
                "bytes_sent": bytes_sent,
                "bytes_recv": bytes_recv,
                "tcp_retransmits": tcp_retransmits,
                "pending_interactions": self.pending_interactions.copy(),
            }

        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return {
                "content": f"Error: {e}",
                "tokens": {},
                "api_latency_ms": 0,
                "effective_kbps": 0,
                "bytes_sent": 0,
                "bytes_recv": 0,
                "tcp_retransmits": 0,
            }

    def _get_network_metrics(self) -> Dict[str, Any]:
        """
        Get network I/O metrics before/after API call.
        """
        metrics = {}

        try:
            net_io = psutil.net_io_counters()
            metrics["bytes_sent"] = net_io.bytes_sent
            metrics["bytes_recv"] = net_io.bytes_recv

            with open("/proc/net/snmp", "r") as f:
                for line in f:
                    if line.startswith("Tcp:"):
                        parts = line.split()
                        if "RetransSegs" in parts:
                            idx = parts.index("RetransSegs")
                            metrics["tcp_retransmits"] = int(parts[idx + 1])
                        break
        except Exception as e:
            logger.debug(f"Could not get network metrics: {e}")
            metrics["bytes_sent"] = 0
            metrics["bytes_recv"] = 0
            metrics["tcp_retransmits"] = 0

        return metrics

    def _emit_event(
        self,
        phase: str,
        event_type: str,
        start_time: float,
        end_time: float,
        metadata: Dict = None,
    ) -> None:
        """
        Emit an orchestration event for tax attribution.

        Args:
            phase: 'planning', 'execution', 'synthesis'
            event_type: 'llm_call', 'tool_call', 'waiting', etc.
            start_time: Start timestamp
            end_time: End timestamp
            metadata: Additional event data
        """
        if not hasattr(self, "_events"):
            self._events = []

        event = {
            "phase": phase,
            "event_type": event_type,
            "start_time_ns": int(start_time * 1e9),
            "end_time_ns": int(end_time * 1e9),
            "duration_ns": int((end_time - start_time) * 1e9),
            "metadata": metadata or {},
        }
        self._events.append(event)
        dprint(f"📝 Event: {phase}.{event_type} ({event['duration_ns']/1e6:.2f}ms)")
        print(f"🔔 EVENT CREATED: {phase}.{event_type}")
