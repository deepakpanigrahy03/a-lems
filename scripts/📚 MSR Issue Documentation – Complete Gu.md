📚 MSR Issue Documentation – Complete Guide
The Problem: Why MSR Access Fails
MSR (Model Specific Register) access is root-only by default on all Linux systems due to security concerns . This affects:

turbostat (needs MSR for C-state data)

rdmsr/wrmsr tools

Any hardware monitoring that reads CPU registers

Symptoms of MSR Issues
Symptom	Error Message
Permission denied	turbostat: Failed to access /dev/cpu/0/msr
No C-state data	turbostat runs but shows 0 columns
rdmsr fails	rdmsr: open: No such file or directory
verify_hardware.py	MSR access: Exists but not readable
The Permanent Fix (What Our Script Does)
Our fix_permissions.sh implements a three-layer permanent solution:

bash
# ============================================================================
# LAYER 1: Group-Based Access (Permanent via udev)
# ============================================================================
# Create a dedicated group
sudo groupadd -f a-lems
sudo usermod -a -G a-lems $USER

# Create udev rule (applies at every boot)
echo 'KERNEL=="msr", GROUP="a-lems", MODE="0440"' | sudo tee /etc/udev/rules.d/99-msr-permissions.rules

# ============================================================================
# LAYER 2: Immediate Fix (Applies Now)
# ============================================================================
# Apply to existing MSR devices
sudo chmod 440 /dev/cpu/*/msr
sudo chown root:a-lems /dev/cpu/*/msr

# Reload udev to apply rules
sudo udevadm control --reload-rules
sudo udevadm trigger --subsystem-match=msr

# ============================================================================
# LAYER 3: Capability-Based Access (For turbostat)
# ============================================================================
# Give turbostat direct MSR access without sudo
sudo setcap cap_sys_rawio=ep $(which turbostat)
Why Reboot is Required
Change	Takes Effect	Reason
Group membership	After logout/login	Groups are set at login time
Udev rules	Immediately + at boot	Kernel applies at device creation
File permissions	Immediately	Direct chmod takes effect now
Capabilities	Immediately	Binary capabilities are instant
Manual Fix (If Ever Needed)
If MSR access breaks again, here's the manual fix:

bash
# Step 1: Check current permissions
ls -la /dev/cpu/0/msr
# Should show: cr--r----- 1 root a-lems

# Step 2: Fix permissions manually
sudo chmod 440 /dev/cpu/*/msr
sudo chown root:a-lems /dev/cpu/*/msr

# Step 3: Verify udev rule exists
cat /etc/udev/rules.d/99-msr-permissions.rules
# Should show: KERNEL=="msr", GROUP="a-lems", MODE="0440"

# Step 4: Reload udev
sudo udevadm control --reload-rules
sudo udevadm trigger --subsystem-match=msr

# Step 5: Check your group membership
groups | grep a-lems
# If missing: sudo usermod -a -G a-lems $USER (then logout/login)

# Step 6: Test MSR access
rdmsr 0x10  # Should return a hex value, not an error
Verification Commands
bash
# Test MSR access
if rdmsr 0x10 2>/dev/null; then
    echo "✅ MSR working"
else
    echo "❌ MSR not accessible"
fi

# Test turbostat with MSR
turbostat --quiet --show all sleep 0.1 2>&1 | head -5

# Check group membership
groups

# Check udev rule
udevadm info --attribute-walk --name=/dev/cpu/0/msr | grep -A5 "looking at device"