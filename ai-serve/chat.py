#!/usr/bin/env python3
"""Smoke-test the local MLX server using the OpenAI-compatible API.

Usage:
    python chat.py "write a python function to reverse a linked list"
"""
import os
import sys

from openai import OpenAI

client = OpenAI(base_url="http://localhost:8080/v1", api_key="not-needed")

# The MLX server uses this field to select/load the model, so it must match
# whatever serve.sh is hosting. Override with MODEL=... if you change models.
MODEL = os.environ.get("MODEL", "mlx-community/Qwen2.5-Coder-3B-Instruct-4bit")

prompt = " ".join(sys.argv[1:]) or "Write a Python function that checks if a string is a palindrome."

stream = client.chat.completions.create(
    model=MODEL,
    messages=[
        {"role": "system", "content": "You are a concise, expert coding assistant."},
        {"role": "user", "content": prompt},
    ],
    stream=True,
    temperature=0.2,
)

for chunk in stream:
    delta = chunk.choices[0].delta.content
    if delta:
        print(delta, end="", flush=True)
print()

