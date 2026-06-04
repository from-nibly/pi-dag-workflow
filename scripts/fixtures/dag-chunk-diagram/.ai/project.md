# Project Brief

## Goal

Use this tiny fixture to test `/dag chunk` and the final Style C DAG diagram output.

## Desired chunking shape

The generated DAG should have visible fan-in and a final dependent validation/docs chunk, rather than only one chunk or a simple two-node chain.

## Validation intent

Success means `/dag chunk` writes `.ai/chunks/*`, writes `.ai/dag.json`, validates the DAG, and prints a compact text diagram showing first-ready chunks, dependencies, and `maxConcurrency`.

## Constraints

- This is a throwaway fixture for manual command testing.
- Do not run `/dag run`; only test chunk generation and diagram output.
