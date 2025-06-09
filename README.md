# fhirFLY

# FHIR HL7 Data Transformation Pipeline

This document outlines the four-step process for converting unstructured healthcare data into FHIR HL7 compliant JSON, leveraging Databricks, Spark, and Large Language Models (LLMs).

---

## Step 1: Initial Information Extraction

This step focuses on taking **unstructured healthcare text** (like endpoint names from health records) and using an LLM to extract key pieces of information into a **structured JSON format**. The LLM is guided to identify potential FHIR resource types (e.g., Organization, Location) and extract relevant fields such as name, address (city, state, country), identifiers, and contact details. It's designed to infer missing data based on common knowledge and return a clean, preliminary JSON output.

---

## Step 2: Contextual Categorization with FHIR Schema

In this step, the **structured JSON output from Step 1** is analyzed by an LLM. To ensure the LLM categorizes the data in a FHIR-aware manner, it's provided with a **dynamically generated list of FHIR resource types and their short descriptions**. This list is sourced directly from your `fhir.resources` package definitions. The LLM then produces a human-readable, "stream of consciousness" summary, explicitly suggesting which FHIR resource types and fields the extracted data might map to, along with potential relationships.

---

## Step 3: Structured FHIR Resource Decision

This step acts as a critical intermediary to refine the LLM's understanding before final FHIR generation. It takes two inputs:
1.  The **initial structured JSON data from Step 1**.
2.  The **natural language categorization and reasoning from Step 2**.

An LLM processes these inputs to identify each major FHIR resource suggested by Step 2. Its output is a **list of dictionaries**, where each dictionary precisely specifies the `fhir_resource_type`, the `relevant_data` (mapped directly from Step 1's JSON), and a concise `reasoning` for the classification. The output is strictly a JSON array, ensuring easy programmatic consumption.

---

## Step 4: Direct FHIR HL7 JSON Generation

This is the final transformation stage. It takes the **structured resource decisions from Step 3** and the **specific FHIR JSON Schema** for each identified resource (retrieved from your `workspace.default.fhir_definitions_table_2`). An LLM is then directly instructed to produce the **final, validated FHIR HL7 compliant JSON object(s)**. The LLM is strictly guided to adhere to the provided schema, map the relevant data, and accept nulls or empty structures for missing fields as per FHIR standards. The output is purely FHIR JSON, ready for ingestion into a FHIR server or further processing.

---

This pipeline systematically refines unstructured data into FHIR, with each step building upon the last, leveraging LLMs for intelligent extraction and categorization while maintaining a clear path to FHIR compliance.
