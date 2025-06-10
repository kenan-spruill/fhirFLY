# fhirFLY

# FHIR HL7 Data Transformation Pipeline

This project provides a pipeline for converting unstructured healthcare data into FHIR HL7 compliant JSON, leveraging Databricks, Spark, and Large Language Models (LLMs).

## Description
- **Reads Unorganized Health Data**: Our system takes messy healthcare information (like notes, scanned documents, old records).
- **AI Understands and Organizes**: It uses smart computer programs (AI) to make sense of this data.
- **Converts to a Standard Format**: It then transforms the data into FHIR HL7, a universal language for health information.
- **Enables Easy Use**: This makes the data clean, organized, and ready for sharing, analysis, and building new healthcare tools.

## Usage

```python
from pyspark.sql import SparkSession
from fhirfly import FHIRFly, process_text_to_fhir

# Initialize SparkSession if not in Databricks
spark = SparkSession.builder.appName("FHIRfly").getOrCreate()

# Method 1: Direct processing
fhir_resources = process_text_to_fhir(
    spark,
    "Nevada Medical and Pain Institute in Las Vegas provides comprehensive pain management services."
)

# Method 2: More control with FHIRFly class
transformer = FHIRFly(
    spark, 
    model_name="databricks-llama-4-maverick",
    fhir_resource_table="workspace.default.fhir_resource_descriptions",
    fhir_definitions_table="workspace.default.fhir_definitions_table_2"
)

# Process text end-to-end
fhir_resources = transformer.transform("Your unstructured healthcare text here")

# Or use individual steps for more control
extracted_data = transformer.step1_extract_information("Your text")
analysis = transformer.step2_analyze_with_fhir_context(extracted_data)
identified_resources = transformer.step3_identify_resources(extracted_data, analysis)
fhir_resources = transformer.step4_generate_fhir_json(identified_resources)
```

## Pipeline Process

### Step 1: Initial Information Extraction

* **Reads Unstructured Data:** Takes messy healthcare text (like notes or endpoint names).
* **AI Extracts Key Info:** Uses an LLM to pull out important details (e.g., name, address, identifiers).
* **Outputs Preliminary JSON:** Provides a clean, structured JSON of the extracted data, even inferring missing details.

### Step 2: Contextual Categorization with FHIR Schema

* **Analyzes Extracted Data:** An LLM reviews the structured JSON from Step 1.
* **Guided by FHIR Definitions:** It receives a dynamic list of FHIR resource types and descriptions (from `fhir.resources`).
* **Categorizes and Suggests:** Produces a human-readable summary, suggesting potential FHIR resource types and fields for the data.

### Step 3: Structured FHIR Resource Decision

* **Refines LLM Understanding:** Combines the initial structured data (Step 1) with the LLM's natural language categorization (Step 2).
* **Identifies Core Resources:** An LLM processes these to pinpoint major FHIR resources.
* **Outputs Structured Decisions:** Generates a JSON list, detailing each resource's type, its relevant data, and the reasoning for its classification.

### Step 4: Direct FHIR HL7 JSON Generation

* **Final Transformation:** Takes the structured resource decisions from Step 3 and the specific FHIR JSON schema.
* **LLM Produces FHIR JSON:** An LLM is directly instructed to create the final, validated FHIR HL7 compliant JSON object(s).
* **Strict Adherence:** The LLM strictly follows the provided schema, mapping data and allowing nulls/empty structures where appropriate.

## Requirements

- Python 3.8+
- Databricks Runtime Environment
- PySpark
- llama-index
- fhir.resources
- Access to LLM endpoints (default: databricks-llama-4-maverick)
