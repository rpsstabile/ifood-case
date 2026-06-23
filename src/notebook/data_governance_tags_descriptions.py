# Databricks notebook source
# DBTITLE 1,Data Governance: Descriptions & Tags for main.ifood
# MAGIC %md
# MAGIC # Data Governance: Descriptions & Tags for main.ifood
# MAGIC
# MAGIC This notebook automates data governance for the `main.ifood` schema:
# MAGIC 1. **Generate descriptions** for all tables and columns using AI
# MAGIC 2. **Generate and apply tags** based on the descriptions
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Access to `main.ifood` schema
# MAGIC - `APPLY TAG` and `COMMENT` privileges on tables

# COMMAND ----------

# DBTITLE 1,Part 1: Generate Table and Column Descriptions
# MAGIC %md
# MAGIC ## Part 1: Generate Table and Column Descriptions
# MAGIC
# MAGIC We'll use Genie (AI functions) to generate meaningful descriptions for all tables and columns.

# COMMAND ----------

# DBTITLE 1,Get all tables in main.ifood
# Get all tables in main.ifood schema
tables_df = spark.sql("""
    SELECT 
        table_catalog,
        table_schema,
        table_name,
        table_type
    FROM system.information_schema.tables
    WHERE table_catalog = 'main' 
    AND table_schema = 'ifood'
    ORDER BY table_name
""")

display(tables_df)

# COMMAND ----------

# DBTITLE 1,Get all columns for each table
# Get all columns for tables in main.ifood
columns_df = spark.sql("""
    SELECT 
        table_catalog,
        table_schema,
        table_name,
        column_name,
        data_type,
        comment
    FROM system.information_schema.columns
    WHERE table_catalog = 'main' 
    AND table_schema = 'ifood'
    ORDER BY table_name, ordinal_position
""")

display(columns_df)

# COMMAND ----------

# DBTITLE 1,Generate descriptions using AI
# Generate descriptions for tables and columns using AI
from pyspark.sql.functions import col, concat_ws, lit

# Prepare table-level descriptions
table_list = tables_df.select("table_name").collect()

print("\n=== Generating Table Descriptions ===")
for row in table_list:
    table_name = row.table_name
    full_table_name = f"main.ifood.{table_name}"
    
    # Get sample data to help AI generate better descriptions
    try:
        sample_data = spark.sql(f"SELECT * FROM {full_table_name} LIMIT 5")
        column_names = ", ".join(sample_data.columns)
        
        # Use AI to generate a description
        description_query = f"""
        SELECT ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            'Generate a concise 1-2 sentence description for a table named "{table_name}" with columns: {column_names}. Focus on the business purpose and data content.'
        ) as description
        """
        
        description = spark.sql(description_query).collect()[0].description
        
        # Escape single quotes in the description
        description = description.replace("'", "''")
        
        # Apply the comment to the table
        spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{description}'")
        print(f"✓ {table_name}: {description}")
        
    except Exception as e:
        print(f"✗ Error processing {table_name}: {str(e)}")

print("\n=== Table Descriptions Complete ===")

# COMMAND ----------

# DBTITLE 1,Generate column descriptions using AI
# Generate column-level descriptions
print("\n=== Generating Column Descriptions ===")

for row in table_list:
    table_name = row.table_name
    full_table_name = f"main.ifood.{table_name}"
    
    # Get columns for this table
    table_columns = columns_df.filter(col("table_name") == table_name).collect()
    
    print(f"\nProcessing table: {table_name}")
    
    for col_row in table_columns:
        column_name = col_row.column_name
        data_type = col_row.data_type
        
        try:
            # Use AI to generate column description
            col_description_query = f"""
            SELECT ai_query(
                'databricks-meta-llama-3-3-70b-instruct',
                'Generate a brief one-sentence description for a column named "{column_name}" (type: {data_type}) in a table called "{table_name}". Describe what data it likely contains.'
            ) as description
            """
            
            col_description = spark.sql(col_description_query).collect()[0].description
            
            # Escape single quotes in the description
            col_description = col_description.replace("'", "''")
            
            # Apply the comment to the column
            spark.sql(f"COMMENT ON COLUMN {full_table_name}.{column_name} IS '{col_description}'")
            print(f"  ✓ {column_name}: {col_description}")
            
        except Exception as e:
            print(f"  ✗ Error processing {column_name}: {str(e)}")

print("\n=== Column Descriptions Complete ===")

# COMMAND ----------

# DBTITLE 1,Part 2: Generate and Apply Tags
# MAGIC %md
# MAGIC ## Part 2: Generate and Apply Tags Based on Descriptions
# MAGIC
# MAGIC Now we'll read all the descriptions we just created and use AI to generate relevant tags for governance, classification, and discovery.

# COMMAND ----------

# DBTITLE 1,Read all table descriptions
# MAGIC %sql
# MAGIC -- Query all tables with their descriptions
# MAGIC SELECT 
# MAGIC     table_catalog,
# MAGIC     table_schema,
# MAGIC     table_name,
# MAGIC     comment as table_description
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'main'
# MAGIC AND table_schema = 'ifood'
# MAGIC AND comment IS NOT NULL
# MAGIC ORDER BY table_name

# COMMAND ----------

# DBTITLE 1,Read all column descriptions
# MAGIC %sql
# MAGIC -- Query all columns with their descriptions
# MAGIC SELECT 
# MAGIC     table_catalog,
# MAGIC     table_schema,
# MAGIC     table_name,
# MAGIC     column_name,
# MAGIC     data_type,
# MAGIC     comment as column_description
# MAGIC FROM system.information_schema.columns
# MAGIC WHERE table_catalog = 'main'
# MAGIC AND table_schema = 'ifood'
# MAGIC AND comment IS NOT NULL
# MAGIC ORDER BY table_name, column_name

# COMMAND ----------

# DBTITLE 1,Generate tags using AI analysis
# Analyze descriptions and generate relevant tags
from pyspark.sql.functions import col, collect_list

# Get all table descriptions
table_descriptions = spark.sql("""
    SELECT 
        table_name,
        comment as description
    FROM system.information_schema.tables
    WHERE table_catalog = 'main'
    AND table_schema = 'ifood'
    AND comment IS NOT NULL
""")

print("\n=== Generating Tags for Tables ===")

for row in table_descriptions.collect():
    table_name = row.table_name
    description = row.description
    full_table_name = f"main.ifood.{table_name}"
    
    try:
        # Use AI to suggest tags based on description
        tag_query = f"""
        SELECT ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            'Based on this table description: "{description}", suggest 3-5 relevant governance tags as key-value pairs. Format as: key1=value1, key2=value2. Use tags for: data_domain, business_unit, quality_tier (bronze/silver/gold), update_frequency, sensitivity_level, or subject_area. Keep keys simple (no spaces).'
        ) as suggested_tags
        """
        
        suggested_tags = spark.sql(tag_query).collect()[0].suggested_tags
        
        print(f"\n{table_name}:")
        print(f"  Description: {description}")
        print(f"  Suggested tags: {suggested_tags}")
        
        # Parse and apply tags
        # Expected format: "key1=value1, key2=value2"
        tag_pairs = suggested_tags.split(",")
        
        for tag_pair in tag_pairs:
            tag_pair = tag_pair.strip()
            if "=" in tag_pair:
                key, value = tag_pair.split("=", 1)
                key = key.strip().replace(" ", "_")
                value = value.strip()
                
                try:
                    # Apply the tag
                    spark.sql(f"SET TAG ON TABLE {full_table_name} {key} = `{value}`")
                    print(f"    ✓ Applied: {key} = {value}")
                except Exception as e:
                    print(f"    ✗ Failed to apply {key}={value}: {str(e)}")
                    
    except Exception as e:
        print(f"✗ Error generating tags for {table_name}: {str(e)}")

print("\n=== Table Tags Complete ===")

# COMMAND ----------

# DBTITLE 1,Generate tags for columns with sensitive data
# Generate tags for columns, focusing on PII and sensitive data
print("\n=== Generating Tags for Columns ===")

column_descriptions = spark.sql("""
    SELECT 
        table_name,
        column_name,
        data_type,
        comment as description
    FROM system.information_schema.columns
    WHERE table_catalog = 'main'
    AND table_schema = 'ifood'
    AND comment IS NOT NULL
""")

for row in column_descriptions.collect():
    table_name = row.table_name
    column_name = row.column_name
    data_type = row.data_type
    description = row.description
    full_table_name = f"main.ifood.{table_name}"
    
    try:
        # Escape double quotes in description for SQL embedding
        safe_description = description.replace('"', '\\"') if description else ""
        
        # Use AI to identify if column contains sensitive data
        sensitivity_query = f"""
        SELECT ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            'Analyze this column: name="{column_name}", type="{data_type}", description="{safe_description}". Suggest relevant tags. Return as: key1=value1, key2=value2. Consider: pii (true/false), pii_type (email/phone/ssn/name/address/etc), sensitivity (high/medium/low), data_classification (public/internal/confidential). Only include relevant tags.'
        ) as suggested_tags
        """
        
        suggested_tags = spark.sql(sensitivity_query).collect()[0].suggested_tags
        
        # Only process if AI suggests tags
        if suggested_tags and len(suggested_tags.strip()) > 0:
            print(f"\n{table_name}.{column_name}:")
            print(f"  Suggested tags: {suggested_tags}")
            
            # Parse and apply tags
            tag_pairs = suggested_tags.split(",")
            
            for tag_pair in tag_pairs:
                tag_pair = tag_pair.strip()
                if "=" in tag_pair:
                    key, value = tag_pair.split("=", 1)
                    key = key.strip().replace(" ", "_")
                    value = value.strip()
                    
                    try:
                        # Apply the tag
                        spark.sql(f"SET TAG ON COLUMN {full_table_name}.{column_name} {key} = `{value}`")
                        print(f"    ✓ Applied: {key} = {value}")
                    except Exception as e:
                        print(f"    ✗ Failed to apply {key}={value}: {str(e)}")
                        
    except Exception as e:
        print(f"✗ Error generating tags for {table_name}.{column_name}: {str(e)}")

print("\n=== Column Tags Complete ===")

# COMMAND ----------

# DBTITLE 1,Verification: View Applied Tags
# MAGIC %md
# MAGIC ## Verification: View All Applied Tags
# MAGIC
# MAGIC Query the tags we just created to verify they were applied correctly.

# COMMAND ----------

# DBTITLE 1,View all table tags
# MAGIC %sql
# MAGIC -- View all table tags in main.ifood
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     tag_name,
# MAGIC     tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE catalog_name = 'main'
# MAGIC AND schema_name = 'ifood'
# MAGIC ORDER BY table_name, tag_name

# COMMAND ----------

# DBTITLE 1,View all column tags
# MAGIC %sql
# MAGIC -- View all column tags in main.ifood
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     column_name,
# MAGIC     tag_name,
# MAGIC     tag_value
# MAGIC FROM system.information_schema.column_tags
# MAGIC WHERE catalog_name = 'main'
# MAGIC AND schema_name = 'ifood'
# MAGIC ORDER BY table_name, column_name, tag_name

# COMMAND ----------

# DBTITLE 1,Summary report
# MAGIC %sql
# MAGIC -- Summary: Tables with descriptions and tag counts
# MAGIC SELECT 
# MAGIC     t.table_name,
# MAGIC     t.comment as description,
# MAGIC     COUNT(DISTINCT tt.tag_name) as num_tags,
# MAGIC     COUNT(DISTINCT c.column_name) as num_columns,
# MAGIC     COUNT(DISTINCT ct.tag_name) as num_column_tags
# MAGIC FROM system.information_schema.tables t
# MAGIC LEFT JOIN system.information_schema.table_tags tt
# MAGIC     ON t.table_catalog = tt.catalog_name
# MAGIC     AND t.table_schema = tt.schema_name
# MAGIC     AND t.table_name = tt.table_name
# MAGIC LEFT JOIN system.information_schema.columns c
# MAGIC     ON t.table_catalog = c.table_catalog
# MAGIC     AND t.table_schema = c.table_schema
# MAGIC     AND t.table_name = c.table_name
# MAGIC LEFT JOIN system.information_schema.column_tags ct
# MAGIC     ON c.table_catalog = ct.catalog_name
# MAGIC     AND c.table_schema = ct.schema_name
# MAGIC     AND c.table_name = ct.table_name
# MAGIC     AND c.column_name = ct.column_name
# MAGIC WHERE t.table_catalog = 'main'
# MAGIC AND t.table_schema = 'ifood'
# MAGIC GROUP BY t.table_name, t.comment
# MAGIC ORDER BY t.table_name