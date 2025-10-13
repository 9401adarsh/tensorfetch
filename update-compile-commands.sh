#!/bin/bash
bear -- make -C ebpf
bear -- make -C daemon
compdb -p ebpf -p daemon list > compile_commands.json
echo "✅ Updated compile_commands.json"