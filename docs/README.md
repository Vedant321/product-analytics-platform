# Architecture Documentation

Data model and dashboard documentation for the Product Analytics Platform.

---

## Architecture Guide

### Platform Architecture

**File:** `ARCHITECTURE.md`

**Shows:**
- Detailed ASCII diagrams
- Table structures and relationships
- Transformation logic
- Query patterns
- Performance metrics

**When to Use:**
- GitHub/GitLab documentation
- Technical deep-dives
- Copy-paste into docs
- When images aren't supported

---

## Diagram Legend

### Bronze Layer (Raw Data)
- Append-only storage
- Immutable audit trail
- Raw CSV/JSON ingestion
- 109.9M events

### Silver Layer (Star Schema)
- **Fact Table:** fact_events (109.9M rows)
- **Dimensions:**
  - dim_date (426 rows)
  - dim_categories (130 rows)
  - dim_products (206.9K rows, SCD Type 2)
  - dim_users (5.3M rows, SCD Type 2)

### Gold Layer (Business Metrics)
- Pre-aggregated tables
- Dashboard-ready
- Fast query performance
- Business-specific grains

---

## File Organization

```
docs/
├── README.md                    # This file
├── ARCHITECTURE.md              # Text-based detailed architecture
├── SILVER_LAYER_ARCHITECTURE.md # Silver layer deep-dive
├── DIMENSIONS_COMPLETE_GUIDE.md # SCD Type 2 implementation
└── assets/dashboard/            # React dashboard screenshots
```

---

## Keeping Diagrams Updated

When updating the architecture:

1. Update `ARCHITECTURE.md` when the data flow or table model changes.
2. Update the root README screenshots when the React dashboard changes.
3. Keep the architecture and deployment guides consistent with the code.

---

## Tips

- Use the dashboard screenshots in the root README for a quick visual overview.
- Use `ARCHITECTURE.md` for data flow, table definitions, and query patterns.

---

**Last Updated:** November 4, 2026
