#!/bin/bash

# Function to count array elements in JSON
count_array() {
    local file=$1
    local key=$2
    grep -o "\"$key\"\s*:\s*\[" "$file" | wc -l
}

echo "=== letters_data.json ==="
head -n 10 "data/funds/ES0112231008/letters_data.json" | grep -E '^\s*"[^"]+": ' | sed 's/.*"\([^"]*\)".*/\1/'
echo ""
echo "Counting cartas array..."
grep '"cartas": \[' "data/funds/ES0112231008/letters_data.json" -A 2000 | grep -c '"periodo":'

echo ""
echo "=== cnmv_data.json - Top keys ==="
head -n 30 "data/funds/ES0112231008/cnmv_data.json" | grep -E '^\s*"[^"]+": ' | sed 's/.*"\([^"]*\)".*/\1/'

echo ""
echo "=== manager_profile.json - Top keys ==="
head -n 20 "data/funds/ES0112231008/manager_profile.json" | grep -E '^\s*"[^"]+": ' | sed 's/.*"\([^"]*\)".*/\1/'

echo ""
echo "=== readings_data.json - Top keys ==="
head -n 30 "data/funds/ES0112231008/readings_data.json" | grep -E '^\s*"[^"]+": ' | sed 's/.*"\([^"]*\)".*/\1/'
