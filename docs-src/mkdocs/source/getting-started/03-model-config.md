# 🤖 Step 9: Model Configuration

Configure API keys for cloud models and test LLM connectivity.

---

## 🔑 API Key Setup

### Option 1: Groq (Recommended - Free Tier)

1. Get your API key from [console.groq.com](https://console.groq.com)
2. Add to `.env` file in project root:

```bash
cat > .env << 'ENVEOF'
GROQ_API_KEY=gsk_your_key_here
ENVEOF
```

### Option 2: OpenRouter

```bash
echo 'OPENROUTER_API_KEY=sk-or-v1-your-key-here' >> .env
```

### Load API Keys

```bash
set -a
source .env
set +a

# Verify
echo $GROQ_API_KEY
```

---

## 📝 Check Model Configuration

The `config/models.json` file defines available models:

```bash
cat config/models.json
```

**Default configuration includes:**

- **Cloud models:** Groq Llama 3.3 70B (linear + agentic)
- **Local models:** TinyLlama 1B (fallback)

---

## 🧪 Test LLM Connectivity

Run the LLM test suite:

```bash
python core/execution/tests/test_llm_setup.py
```

**Expected output:**

```
======================================================================
LLM SETUP TEST RESULTS
======================================================================

✅ CLOUD_LINEAR
   Provider: groq
   Model: Groq Llama 3.3 70B
   Response: [actual response]

✅ CLOUD_AGENTIC
   Provider: groq
   Model: Groq Llama 3.3 70B (Agentic)
   Response: [actual response]

✅ LOCAL_LINEAR
   Provider: local
   Model: TinyLlama 1B Offline
   Response: [actual response]

✅ LOCAL_AGENTIC
   Provider: local
   Model: TinyLlama 1B Offline (Agentic)
   Response: [actual response]
```

---

## ⚠️ Troubleshooting LLM Issues

| Error | Solution |
|-------|----------|
| `401 Unauthorized` | API key invalid - check `.env` file |
| `No API key found` | Keys not loaded - run `source .env` |
| `ModuleNotFoundError` | Install missing deps: `pip install -r requirements.txt` |
| `execution_time_ms error` | Update to latest code: `git pull` |

---

## 🔄 Next Step

Proceed to **[Quick Start Guide](04-quick-start.md)** to run your first experiment!