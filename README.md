## 🥉 Bronze Layer – Incremental Ingestion

This pipeline loads data incrementally from Azure SQL to ADLS Bronze layer using a CDC watermark mechanism.-

### Flow:
Lookup (last_cdc)  
→ Set Variable (current)  
→ Copy Data (AzureSQLToLake)  
→ If Condition (IfIncrementalData)

### What it does:
- Reads last processed timestamp from `cdc.json`
- Loads only new/updated records using `updated_at > last_cdc`
- Calculates latest MAX(updated_at)
- Updates watermark file after successful run
- Skips update if no new data

### Key Points:
- Incremental load logic
- No hardcoding
- Safe watermark handling
- Production-style ingestion design
  <img width="1297" height="562" alt="image" src="https://github.com/user-attachments/assets/f7d3bb88-27db-4306-adf8-4a062a3b6e87" />

  ## 🥉 Bronze Layer – Incremental Loop (Multi-Table Ingestion)

This pipeline extends the incremental ingestion logic to support multiple tables using a ForEach activity.

### Flow:
ForEach (loop_input)
→ Lookup (last_cdc)
→ Copy Data (AzureSQLToLake)
→ If Condition (IfIncrementalData)
→ Web Activity (Alerts)

### What it does:
- Runs incremental load across multiple tables
- Uses parameterized schema, table, and CDC column
- Maintains separate watermark logic per table
- Updates watermark only when new data is detected
- Handles empty loads safely
- Triggers alert (Logic App) if pipeline fails

### Key Points:
- Fully parameterized design
- Multi-table incremental ingestion
- Failure monitoring & alert integration
- Production-ready structure
  <img width="1357" height="449" alt="image" src="https://github.com/user-attachments/assets/850829e0-874f-4c5c-bc2e-b49f08d0d7f6" />

  ## 🥈 Silver Layer – Streaming Transformations

The Silver layer processes raw Bronze data using Azure Databricks Structured Streaming and writes clean Delta tables.

### What it does:
- Reads data from Bronze (ADLS) using Auto Loader (`cloudFiles`)
- Uses streaming with `trigger(once=True)`
- Writes data in Delta format
- Maintains checkpointing for reliability
- Registers tables in Unity Catalog

### Transformations Applied:
- Removed `_rescued_data` column
- Standardized schema
- Cleaned raw ingestion artifacts
- Extracted required date fields (where applicable)

### Architecture:
Read Bronze (Streaming)  
→ Apply Transformations  
→ Write to Silver (Delta)  
→ Checkpoint Tracking  

### Delta Features Used:
- ACID transactions  
- Schema enforcement  
- Transaction log (`_delta_log`)  
- Append mode writes  

### Final Output:
Catalog: `spotify_cata`  
Schema: `silver`  
Tables:
- dimartist  
- dimuser  
- dimtrack  
- dimdate  
- factstream  

✔ Cleaned  
✔ Structured  
✔ Delta optimized  
✔ Ready for Gold layer

## 🥇 Gold Layer – Incremental + SCD Type 2

The Gold layer builds business-ready dimension tables using incremental processing and SCD Type 2 logic.

### What Happens in This Layer

1. Data is read from Silver tables.
2. Only new or updated records are selected using timestamp-based incremental filtering.
3. A Delta `MERGE` statement is executed.
4. If a record has changed:
   - Old version is marked inactive (`is_current = false`)
   - New version is inserted
5. If the record is new:
   - It is inserted directly.

---

### Incremental Behavior Observed

- First run: 500 records processed.
- After new data was added (total 505 in Silver),
- Only 5 new/changed records were processed in Gold.

This confirms that:

- No full reload was performed
- Only delta changes were merged
- Historical records were preserved

---

### Result

✔ Incremental load  
✔ SCD Type 2 implemented  
✔ History maintained  
✔ Efficient Delta MERGE processing  
✔ Business-ready dimension tables  
<img width="977" height="789" alt="image" src="https://github.com/user-attachments/assets/09380d36-d34e-40c6-8d34-0dded83d6e1f" />





