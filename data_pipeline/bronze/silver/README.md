## Silver Layer

The Silver layer transforms raw Bronze data into structured, analytics-ready datasets using Databricks Structured Streaming and Delta Lake.

Data was read from the Bronze layer in ADLS using Auto Loader (`cloudFiles`) in streaming mode (trigger once).

Each table followed this flow:

Bronze Data → Apply Cleaning & Standardization → Write to Silver Delta → Maintain Checkpoint

Transformations included:

- Removing `_rescued_data` (schema drift artifacts)
- Extracting date fields from timestamps
- Standardizing schema for consistency

Each table maintained its own checkpoint to ensure:

- Fault tolerance  
- Exactly-once processing  

Silver data was written in Delta format and later registered in Unity Catalog using external table registration.

Final Silver tables:

- dimartist  
- dimuser  
- dimtrack  
- dimdate  
- factstream  

These cleaned and structured datasets were prepared for downstream Gold aggregations.
