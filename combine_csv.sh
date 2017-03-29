#!/bin/bash

directory=$1
if [[ ! -d "$directory" ]]; then
    echo "Cannot find directory '$directory'"
    exit 1
fi
files=(`ls -S "$directory"/*.csv`)
echo "Combining csv files: ${files[@]}"
for i in "${!files[@]}"; do
    if [[ $i == 0 ]]; then
        cat "${files[i]}" > "$directory/combined.csv"
    else
        tail -n +2 "${files[i]}" >> "$directory/combined.csv"
    fi
done