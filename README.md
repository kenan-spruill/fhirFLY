# fhirFLY

# FHIR HL7 Data Transformation Pipeline

This document outlines the four-step process for converting unstructured healthcare data into FHIR HL7 compliant JSON, leveraging Databricks, Spark, and Large Language Models (LLMs).

## Description
- Reads Unorganized Health Data: Our system takes messy healthcare information (like notes, scanned documents, old records).
- AI Understands and Organizes: It uses smart computer programs (AI) to make sense of this data.
- Converts to a Standard Format: It then transforms the data into FHIR HL7, a universal language for health information.
- Enables Easy Use: This makes the data clean, organized, and ready for sharing, analysis, and building new healthcare tools.

---

### Step 1: Initial Information Extraction

* **Reads Unstructured Data:** Takes messy healthcare text (like notes or endpoint names).
* **AI Extracts Key Info:** Uses an LLM to pull out important details (e.g., name, address, identifiers).
* **Outputs Preliminary JSON:** Provides a clean, structured JSON of the extracted data, even inferring missing details.

---

### Step 2: Contextual Categorization with FHIR Schema

* **Analyzes Extracted Data:** An LLM reviews the structured JSON from Step 1.
* **Guided by FHIR Definitions:** It receives a dynamic list of FHIR resource types and descriptions (from `fhir.resources`).
* **Categorizes and Suggests:** Produces a human-readable summary, suggesting potential FHIR resource types and fields for the data.

---

### Step 3: Structured FHIR Resource Decision

* **Refines LLM Understanding:** Combines the initial structured data (Step 1) with the LLM's natural language categorization (Step 2).
* **Identifies Core Resources:** An LLM processes these to pinpoint major FHIR resources.
* **Outputs Structured Decisions:** Generates a JSON list, detailing each resource's type, its relevant data, and the reasoning for its classification.

---

### Step 4: Direct FHIR HL7 JSON Generation

* **Final Transformation:** Takes the structured resource decisions from Step 3 and the specific FHIR JSON schema.
* **LLM Produces FHIR JSON:** An LLM is directly instructed to create the final, validated FHIR HL7 compliant JSON object(s).
* **Strict Adherence:** The LLM strictly follows the provided schema, mapping data and allowing nulls/empty structures where appropriate.
---

This pipeline systematically refines unstructured data into FHIR, with each step building upon the last, leveraging LLMs for intelligent extraction and categorization while maintaining a clear path to FHIR compliance.
