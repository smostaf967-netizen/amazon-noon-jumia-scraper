# E-commerce Price Intelligence

Automated product catalog monitoring and price tracking across major Egyptian e-commerce platforms.

## Overview

This project collects and normalizes product data from public e-commerce listings to support
market analysis, pricing research, and competitive benchmarking.

**Platforms:** Amazon Egypt · Noon Egypt · Jumia Egypt

## Architecture

Distributed data pipeline using GitHub Actions for scheduled job orchestration:

- **Catalog Monitor** — Tracks product listings across category groups
- **Node Monitor**    — Deep catalog traversal via browse node hierarchy
- **Related Monitor** — Cross-product relationship mapping

## Output Format

Structured CSV datasets with fields for product identifier, title, pricing,
category, and availability metadata.

## Stack

- Python 3.11+
- GitHub Actions (distributed scheduling)
- CSV / JSON (data storage)
