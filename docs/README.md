# Architecture Diagrams

Visual diagrams for the Product Analytics Platform.

---

## 📊 Available Diagrams

### 1. Full Platform Architecture (Image)

**File:** `architecture-diagram.png` (saved separately)

**Shows:**
- Complete 3-layer architecture (Bronze → Silver → Gold)
- Star schema visualization
- Data flow between layers
- Cross-cutting features (data quality, monitoring, lineage, etc.)
- Storage & infrastructure components
- Access control

**When to Use:**
- Presentations and documentation
- Onboarding new team members
- Architecture reviews
- High-level system overview

---

### 2. Text-Based Architecture (Markdown)

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

## 🎨 Diagram Legend

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

## 📁 File Organization

```
docs/
├── README.md                    # This file
├── architecture-diagram.png     # Visual diagram (image format)
├── ARCHITECTURE.md              # Text-based detailed architecture
├── SILVER_LAYER_ARCHITECTURE.md # Silver layer deep-dive
└── DIMENSIONS_COMPLETE_GUIDE.md # SCD Type 2 implementation
```

---

## 🔄 Keeping Diagrams Updated

When updating the architecture:

1. **Image Diagram:** Update the source image file
2. **Text Diagrams:** Update ARCHITECTURE.md
3. **Cross-reference:** Ensure both stay in sync
4. **Commit both:** Keep visual and text versions together

---

## 💡 Tips

- Use the **image** for presentations and quick overviews
- Use the **text markdown** for technical documentation
- Both represent the same architecture, just different formats
- The text version (ARCHITECTURE.md) has more detailed explanations

---

**Last Updated:** August 10, 2026
