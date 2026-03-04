#!/bin/bash
"""
================================================================================
A-LEMS Debug Runner - Run any script with debug enabled
================================================================================

Usage:
    ./scripts/debug_run.sh core/readers/msr_reader.py
    ./scripts/debug_run.sh --modules=msr_reader,rapl_reader core/readers/msr_reader.py
    ./scripts/debug_run.sh --file=/tmp/debug.log core/readers/msr_reader.py

Environment variables set by this script:
    A_LEMS_DEBUG=1
    A_LEMS_DEBUG_MODULES=...
    A_LEMS_DEBUG_FILE=...
"""

# Colors for help text
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
MODULES=""
DEBUG_FILE=""
SCRIPT=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --modules=*)
            MODULES="${1#*=}"
            shift
            ;;
        --file=*)
            DEBUG_FILE="${1#*=}"
            shift
            ;;
        --help|-h)
            echo -e "${GREEN}A-LEMS Debug Runner${NC}"
            echo ""
            echo "Usage: $0 [OPTIONS] SCRIPT"
            echo ""
            echo "Options:"
            echo "  --modules=MODULE1,MODULE2  Debug only specific modules"
            echo "  --file=FILE                Write debug output to file"
            echo "  --help, -h                  Show this help"
            echo ""
            echo "Examples:"
            echo "  $0 core/readers/msr_reader.py"
            echo "  $0 --modules=msr_reader core/readers/msr_reader.py"
            echo "  $0 --file=/tmp/debug.log core/readers/msr_reader.py"
            exit 0
            ;;
        -*)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
        *)
            SCRIPT="$1"
            shift
            ;;
    esac
done

# Check if script is provided
if [ -z "$SCRIPT" ]; then
    echo -e "${RED}Error: No script specified${NC}"
    echo "Usage: $0 [OPTIONS] SCRIPT"
    exit 1
fi

# Check if script exists
if [ ! -f "$SCRIPT" ]; then
    echo -e "${RED}Error: Script not found: $SCRIPT${NC}"
    exit 1
fi

# Set environment variables
export A_LEMS_DEBUG=1

if [ -n "$MODULES" ]; then
    export A_LEMS_DEBUG_MODULES="$MODULES"
    echo -e "${YELLOW}🐛 Debug enabled for modules: $MODULES${NC}"
else
    echo -e "${YELLOW}🐛 Debug enabled for ALL modules${NC}"
fi

if [ -n "$DEBUG_FILE" ]; then
    export A_LEMS_DEBUG_FILE="$DEBUG_FILE"
    echo -e "${YELLOW}📝 Debug output to: $DEBUG_FILE${NC}"
fi

# Create debug file directory if needed
if [ -n "$DEBUG_FILE" ]; then
    mkdir -p "$(dirname "$DEBUG_FILE")"
fi

echo -e "${GREEN}🚀 Running: $SCRIPT${NC}"
echo ""

# Run the script
python "$SCRIPT"

# Check exit status
if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}✅ Script completed successfully${NC}"
else
    echo -e "\n${RED}❌ Script failed with exit code $?${NC}"
fi