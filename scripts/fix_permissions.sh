#!/bin/bash
# A-LEMS Universal Permission Fixer
# Run ONCE with sudo and everything is fixed forever

set -e

echo "================================================================="
echo "A-LEMS Universal Permission Fixer"
echo "================================================================="

# ============================================================================
# DETECT SYSTEM
# ============================================================================
if [ -f /etc/os-release ]; then
    . /etc/os-release
    DISTRO=$ID
else
    DISTRO="unknown"
fi

CPU_VENDOR=$(lscpu | grep "Vendor ID" | awk '{print $3}' 2>/dev/null || echo "unknown")

# ============================================================================
# 1. RAPL PERMISSIONS (Intel only)
# ============================================================================
echo -e "\n[1/4] Fixing RAPL permissions..."

if [ -d "/sys/class/powercap" ] && ls /sys/class/powercap/intel-rapl* 1> /dev/null 2>&1; then
    cat > /tmp/rapl-permissions.service << 'EOF'
[Unit]
Description=A-LEMS RAPL Permission Fix
After=sysinit.target

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'chmod 0755 /sys/class/powercap/intel-rapl* 2>/dev/null; chmod 0755 /sys/devices/virtual/powercap/intel-rapl* 2>/dev/null; chmod 0444 /sys/class/powercap/intel-rapl*/energy_uj 2>/dev/null; chmod 0444 /sys/devices/virtual/powercap/intel-rapl/*/energy_uj 2>/dev/null'
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
    sudo mv /tmp/rapl-permissions.service /etc/systemd/system/
    sudo systemctl enable rapl-permissions.service
    sudo systemctl start rapl-permissions.service
    echo "  ✅ RAPL systemd service created"
else
    echo "  ⚠️ RAPL not available, skipping"
fi

# ============================================================================
# 1.5 UNCORE FREQUENCY PERMISSIONS - NEW SECTION ADDED
# ============================================================================
# Why: The current_freq_khz file shows the live ring bus frequency, but is
#      typically root-readable only (Issue #3). Making it world-readable allows
#      A-LEMS to read it without sudo.
#      Using hw_config.json ensures we fix the EXACT paths for this system.
echo -e "\n[1.5/4] Fixing uncore frequency permissions..."

# Check if jq is installed (needed to parse JSON config)
if ! command -v jq &> /dev/null; then
    echo "  ⚠️ jq not found. Installing..."
    sudo apt-get update && sudo apt-get install -y jq
fi

# Read paths from config if available
if [ -f "config/hw_config.json" ]; then
    echo "  Reading paths from config/hw_config.json..."
    
    # Get absolute path to config for systemd service
    CONFIG_ABS_PATH=$(realpath config/hw_config.json 2>/dev/null || echo "/home/dpani/mydrive/a-lems/config/hw_config.json")
    
    # Extract all sysfs_paths values using jq
    # The '?' handles missing keys gracefully
    paths=$(jq -r '.ring_bus.sysfs_paths[]?' config/hw_config.json 2>/dev/null)
    
    if [ -n "$paths" ]; then
        echo "$paths" | while read path; do
            if [ -n "$path" ] && [ -e "$path" ]; then
                # Check current permissions
                current_perms=$(stat -c "%a" "$path" 2>/dev/null)
                if [ "$current_perms" != "644" ]; then
                    echo "    Fixing: $path (was $current_perms)"
                    sudo chmod 0644 "$path"
                else
                    echo "    ✅ Already correct: $path"
                fi
            fi
        done
        
        # Also find and fix any other freq files in the same directories
        # This catches any related files that might need similar permissions
        dirs=$(jq -r '.ring_bus.sysfs_paths[]?' config/hw_config.json 2>/dev/null | xargs -n1 dirname | sort -u)
        echo "$dirs" | while read dir; do
            if [ -n "$dir" ] && [ -d "$dir" ]; then
                for freq_file in "$dir"/*freq*; do
                    if [ -f "$freq_file" ]; then
                        current_perms=$(stat -c "%a" "$freq_file" 2>/dev/null)
                        if [ "$current_perms" != "644" ]; then
                            echo "    Fixing related: $freq_file"
                            sudo chmod 0644 "$freq_file"
                        fi
                    fi
                done
            fi
        done
        
        echo "  ✅ Uncore frequency permissions fixed from config"
        
        # Create systemd service to persist across reboots
        # This ensures permissions are reapplied every time the system starts
        cat > /tmp/uncore-permissions.service << EOF
[Unit]
Description=A-LEMS Uncore Frequency Permission Fix
After=sysinit.target

[Service]
Type=oneshot
ExecStart=/bin/bash -c 'CONFIG_PATH="$CONFIG_ABS_PATH"; if [ -f "\$CONFIG_PATH" ] && command -v jq &> /dev/null; then paths=\$(jq -r ".ring_bus.sysfs_paths[]?" "\$CONFIG_PATH" 2>/dev/null); echo "\$paths" | while read path; do if [ -n "\$path" ] && [ -e "\$path" ]; then chmod 0644 "\$path"; fi; done; fi'
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
        sudo mv /tmp/uncore-permissions.service /etc/systemd/system/
        sudo systemctl enable uncore-permissions.service
        sudo systemctl start uncore-permissions.service
        echo "  ✅ Uncore frequency systemd service created"
        
    else
        echo "  ⚠️ No sysfs_paths found in ring_bus section of config"
        echo "  Run hardware detection again to populate paths"
    fi
else
    echo "  ⚠️ config/hw_config.json not found"
    echo "  Run detection first: sudo python scripts/detect_hardware.py --output config/hw_config.json --merge"
fi

# ============================================================================
# 2. MSR PERMISSIONS
# ============================================================================
echo -e "\n[2/4] Fixing MSR permissions..."

# Create group
sudo groupadd -f a-lems
sudo usermod -a -G a-lems $USER
echo "  ✅ Group 'a-lems' ensured"

# Create udev rule
echo 'KERNEL=="msr", GROUP="a-lems", MODE="0440"' | sudo tee /etc/udev/rules.d/99-msr-permissions.rules > /dev/null
echo "  ✅ Udev rule created"

# Apply to existing MSR devices
for cpu in /dev/cpu/*/msr; do
    if [ -e "$cpu" ]; then
        sudo chmod 440 $cpu
        sudo chown root:a-lems $cpu
    fi
done
echo "  ✅ Permissions applied to existing MSR devices"

# Reload udev
sudo udevadm control --reload-rules
sudo udevadm trigger --subsystem-match=msr 2>/dev/null || true
echo "  ✅ Udev rules reloaded"

# ============================================================================
# CRITICAL: Set capabilities for rdmsr tool
# ============================================================================
if command -v rdmsr &> /dev/null; then
    sudo setcap cap_sys_rawio=ep $(which rdmsr)
    echo "  ✅ rdmsr capabilities set"
else
    echo "  ⚠️ rdmsr not found, install with: sudo apt install msr-tools"
fi

# Also set for turbostat if needed
if command -v turbostat &> /dev/null; then
    sudo setcap cap_sys_rawio=ep $(which turbostat) 2>/dev/null || true
    echo "  ✅ turbostat capabilities set"
fi

# ============================================================================
# 3. PERF_EVENT
# ============================================================================
echo -e "\n[3/4] Fixing perf_event permissions..."

echo 'kernel.perf_event_paranoid = -1' | sudo tee /etc/sysctl.d/99-a-lems.conf > /dev/null
sudo sysctl -p /etc/sysctl.d/99-a-lems.conf > /dev/null
echo "  ✅ perf_event_paranoid set to -1"

# ============================================================================
# 4. TURBOSTAT
# ============================================================================
echo -e "\n[4/4] Setting turbostat capabilities on REAL binary..."

REAL_TURBOSTAT=""

# Try to read from config first
if [ -f "config/hw_config.json" ]; then
    REAL_TURBOSTAT=$(python3 -c "
import json
try:
    with open('config/hw_config.json') as f:
        config = json.load(f)
        print(config.get('turbostat', {}).get('real_binary', ''))
except:
    print('')
" 2>/dev/null)
fi

# If not found in config, try to auto-detect
if [ -z "$REAL_TURBOSTAT" ] || [ ! -f "$REAL_TURBOSTAT" ]; then
    KERNEL=$(uname -r)
    if [ -f "/usr/lib/linux-tools/$KERNEL/turbostat" ]; then
        REAL_TURBOSTAT="/usr/lib/linux-tools/$KERNEL/turbostat"
    else
        # Try to follow symlinks from wrapper
        WRAPPER=$(which turbostat 2>/dev/null)
        if [ -L "$WRAPPER" ]; then
            REAL_TURBOSTAT=$(readlink -f "$WRAPPER")
        fi
    fi
fi

if [ -n "$REAL_TURBOSTAT" ] && [ -f "$REAL_TURBOSTAT" ]; then
    echo "  Found real turbostat at: $REAL_TURBOSTAT"
    sudo setcap cap_sys_rawio=ep "$REAL_TURBOSTAT" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "  ✅ turbostat capabilities set on REAL binary"
    else
        echo "  ⚠️ Could not set capabilities"
    fi
else
    echo "  ⚠️ Could not find real turbostat binary"
fi

# ============================================================================
# 2.5 MSR HELPER BINARY - COMPILE AND SET SUID
# ============================================================================
echo -e "\n[2.5/4] Compiling MSR helper binary..."

MSR_DIR="core/msr_helper"
MSR_HELPER="$MSR_DIR/msr_read"

    
    # Check if source exists
    if [ -f "$MSR_DIR/msr_read.c" ]; then
        cd "$MSR_DIR"
        make clean 2>/dev/null || true   # Remove old binary
        make 2>/dev/null || gcc -o msr_read msr_read.c
        cd - > /dev/null
        echo "  ✅ MSR helper compiled"
        # Verify
        ls -la "$MSR_HELPER"
    else
        echo "  ❌ MSR source not found at $MSR_DIR/msr_read.c"
        echo "  Please restore the source file from git or backup"
        exit 1
    fi


# Verify binary exists
if [ -f "$MSR_HELPER" ]; then
    echo "  ✅ Binary found at $MSR_HELPER"
else
    echo "  ❌ Failed to compile MSR helper"
    exit 1
fi

# ============================================================================
# 2.6 MSR HELPER BINARY - SET SUID
# ============================================================================
echo -e "\n[2.6/4] Setting SUID on MSR helper binary..."

if [ -f "$MSR_HELPER" ]; then
    sudo chown root:root "$MSR_HELPER"
    sudo chmod u+s "$MSR_HELPER"
    echo "  ✅ SUID set on $MSR_HELPER"
    
    # Verify
    ls -la "$MSR_HELPER"
else
    echo "  ⚠️ MSR helper not found at $MSR_HELPER"
fi


# ============================================================================
# FINAL MESSAGE
# ============================================================================
echo -e "\n================================================================="
echo "✅ PERMISSIONS FIXED!"
echo "================================================================="
echo ""
echo "⚠️  IMPORTANT: Log out and log back in NOW!"
echo ""
echo "After logging back in, run detection:"
echo "  python scripts/detect_hardware.py --output config/hw_config.json --merge --verbose"
echo "================================================================="