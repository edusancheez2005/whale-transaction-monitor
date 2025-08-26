#!/bin/bash

# Whale Transaction Monitor Startup Script
# Provides default input to avoid TTY interaction issues

echo "🚀 Starting Whale Transaction Monitor..."

# Provide default minimum value to avoid interactive prompt
echo "5000" | nohup python enhanced_monitor.py > whale_monitor.log 2>&1 &

MONITOR_PID=$!

echo "✅ Whale Monitor started with PID: $MONITOR_PID"
echo "📊 Log file: whale_monitor.log"
echo "⏹️  To stop: kill $MONITOR_PID"

# Save PID for easy stopping later
echo $MONITOR_PID > whale_monitor.pid

echo "🔍 Monitoring status:"
sleep 2
if ps -p $MONITOR_PID > /dev/null 2>&1; then
    echo "✅ Whale Monitor is running successfully!"
    echo "📈 View live logs: tail -f whale_monitor.log"
else
    echo "❌ Whale Monitor failed to start - check whale_monitor.log"
fi 