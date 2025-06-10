"""
FHIRfly - A tool for generating FHIR resources from unstructured health text

This module provides functionality to transform unstructured healthcare text
into FHIR HL7 compliant JSON resources using LLMs.
"""

import json
from typing import List, Dict, Any, Optional, Union, Tuple
from pprint import pprint

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from llama_index.core.llms import ChatMessage
from llama_index.llms.databricks import Databricks
from databricks.sdk import WorkspaceClient

transcription_example = """

---

## Doctor's Dictation: Follow-up Visit

**PATIENT:** Jane Doe, DOB: 05/15/1970
**DATE OF SERVICE:** June 9, 2025

**SUBJECTIVE:**
Patient presents for a scheduled follow-up regarding her hypertension and Type 2 Diabetes Mellitus. Reports feeling generally well. Denies chest pain, shortness of breath, dizziness, or new neurological symptoms. No complaints of polyuria, polydipsia, or blurry vision. States she has been compliant with her medications. Reports checking blood sugars three times a week, with readings mostly in the 120-140 mg/dL range fasting. Blood pressure readings at home typically 130s/80s mmHg. Reports walking 30 minutes, 4-5 times a week. Diet remains mostly plant-based, limiting processed foods. Denies any new illnesses or recent hospitalizations.

**OBJECTIVE:**
* **Vitals:** BP 132/84 mmHg, HR 72 bpm, RR 16, Temp 98.6 F, O2 Sat 99% on room air. Weight 165 lbs (down 2 lbs from last visit). BMI 26.5 kg/m².
* **General:** Alert and oriented x 3, well-nourished, no acute distress.
* **HEENT:** Normocephalic, atraumatic. Conjunctivae anicteric. Pupils equal, round, reactive to light and accommodation. Oropharynx clear, moist.
* **Neck:** Supple, no lymphadenopathy, no thyromegaly.
* **Cardiovascular:** Regular rate and rhythm, S1/S2 audible, no murmurs, gallops, or rubs. Peripheral pulses 2+ and symmetric. No edema.
* **Pulmonary:** Lungs clear to auscultation bilaterally. No wheezes, rales, or rhonchi. Good air movement.
* **Abdomen:** Soft, non-tender, non-distended. Bowel sounds normoactive in all four quadrants. No hepatosplenomegaly.
* **Extremities:** No clubbing, cyanosis, or edema. Full range of motion.
* **Neurological:** Grossly intact. Cranial nerves II-XII intact. Sensation intact to light touch. Deep tendon reflexes 2+ bilaterally.

**ASSESSMENT:**
1.  **Hypertension, essential:** Well-controlled on current medication regimen, with home and office readings within target range. Patient remains compliant.
2.  **Type 2 Diabetes Mellitus:** Appears well-controlled with current diet, exercise, and medication. Home blood glucose monitoring within acceptable range. No signs of acute complications.
3.  **Obesity:** Patient continues to make progress with weight management through diet and exercise.

**PLAN:**
1.  **Hypertension:**
    * Continue Lisinopril 20 mg daily.
    * Continue Amlodipine 5 mg daily.
    * Encourage continued home BP monitoring.
2.  **Type 2 Diabetes Mellitus:**
    * Continue Metformin 1000 mg twice daily.
    * Continue healthy diet and exercise.
    * Order HBA1c for next visit.
3.  **Obesity:**
    * Continue current diet and exercise plan.
    * Encourage continued weight loss.
4.  **Labs:** Order comprehensive metabolic panel (CMP), lipid panel, and HBA1c to be drawn one week prior to next appointment.
5.  **Follow-up:** Return in 3 months for recheck of labs and clinical status.
---
"""
from library import get_all_fhir_resource_info_with_descriptions


class FHIRFly:
    """
    Main class for transforming unstructured health text to FHIR HL7 resources.
    
    This implements a 4-step pipeline:
    1. Extract structured data from unstructured text
    2. Categorize and analyze data using FHIR context
    3. Identify core FHIR resources and attributes
    4. Generate FHIR HL7 compliant JSON
    """
    
    def __init__(
        self,
        spark: SparkSession,
        model_name: str = "databricks-llama-4-maverick",
        fhir_resource_table: str = "workspace.default.fhir_resource_descriptions",
        fhir_definitions_table: str = "workspace.default.fhir_definitions_table_2",
        token_lifetime_seconds: int = 14400
    ):
        """
        Initialize the FHIRFly transformer.
        
        Args:
            spark: Active SparkSession
            model_name: Databricks LLM model name to use
            fhir_resource_table: Delta table containing FHIR resource descriptions
            fhir_definitions_table: Delta table containing FHIR JSON schemas
            token_lifetime_seconds: Lifetime of the Databricks token in seconds
        """
        self.spark = spark
        self.model_name = model_name
        self.fhir_resource_table = fhir_resource_table
        self.fhir_definitions_table = fhir_definitions_table
        
        # Initialize Databricks client and LLM
        self.workspace_client = WorkspaceClient()
        tmp_token = self.workspace_client.tokens.create(
            comment="for FHIR transformation", 
            lifetime_seconds=token_lifetime_seconds
        )
        
        self.llm = Databricks(
            model=model_name,
            api_key=tmp_token.token_value,
            api_base=f"{self.workspace_client.config.host}/serving-endpoints/"
        )
        
        # Load FHIR resource descriptions and schemas
        self._load_fhir_resources()
    
    def _load_fhir_resources(self) -> None:
        """Load FHIR resource descriptions and schemas from Delta tables."""
        try:
            # Load FHIR resource descriptions
            fhir_df = self.spark.read.format("delta").table(self.fhir_resource_table)
            self.fhir_resource_descriptions = fhir_df.toPandas().to_dict(orient='records')
            
            # Load FHIR schemas
            self.schema_df = self.spark.read.format("delta").table(self.fhir_definitions_table).toPandas()
        except Exception as e:
            raise RuntimeError(f"Failed to load FHIR resources: {str(e)}")
    
    def _format_fhir_context(self) -> str:
        """Format FHIR resource descriptions for LLM context."""
        context = (
            "Refer to the following list of FHIR Resource Types and their short descriptions "
            "to help you categorize the extracted information. Pay attention to the purpose "
            "of each resource to make the best classification:\n\n"
        )
        
        for res_info in self.fhir_resource_descriptions:
            context += (
                f"- **{res_info['fhir_resource_name']}**: {res_info['fhir_resource_description']}\n"
            )
        
        return context + "\n"
    
    def _extract_json_from_llm_output(self, raw_output: str) -> Any:
        """
        Extract JSON from LLM output, handling different output formats.
        
        Args:
            raw_output: Raw text output from LLM
            
        Returns:
            Parsed JSON object
        
        Raises:
            json.JSONDecodeError: If JSON parsing fails
        """
        # First attempt: direct JSON parsing
        try:
            return json.loads(raw_output)
        except json.JSONDecodeError:
            pass
        
        # Second attempt: extract from markdown code blocks
        if '```json' in raw_output and '```' in raw_output:
            json_start = raw_output.find('```json') + len('```json')
            json_end = raw_output.rfind('```')
            json_string = raw_output[json_start:json_end].strip()
            return json.loads(json_string)
        
        # Third attempt: find JSON-like structure using braces
        json_chars = {'[': ']', '{': '}'}
        for start_char, end_char in json_chars.items():
            first_pos = raw_output.find(start_char)
            last_pos = raw_output.rfind(end_char)
            
            if first_pos != -1 and last_pos != -1 and last_pos > first_pos:
                json_string = raw_output[first_pos:last_pos + 1].strip()
                return json.loads(json_string)
        
        # If all attempts fail, try one more time with the cleaned output
        cleaned_output = raw_output.strip()
        return json.loads(cleaned_output)
    
    def step1_extract_information(self, text: str) -> List[Dict[str, Any]]:
        """
        Step 1: Extract structured information from unstructured text.
        
        Args:
            text: Unstructured healthcare text to process
            
        Returns:
            List of dictionaries with structured extracted information
        """
        system_message = (
            "You are an expert at extracting FHIR data from unstructured healthcare text. "
            "Your goal is to parse the provided text and identify information relevant to a FHIR resource. "
            "You should determine the most appropriate FHIR resource type (e.g., Location, Practitioner, Organization, Patient) "
            "based on the input text. "
            "Then, extract key fields for that resource, such as 'name', 'address' (with 'city', 'state', 'country'), 'identifier', 'telecom', etc. "
            "For 'address', always include 'city', 'state', and 'country'. "
            "Infer and guess data based on common knowledge (e.g., 'California' for 'Los Angeles', 'USA' for US states). "
            "Return the extracted information as a JSON array, where each element is a FHIR-compliant JSON object. "
            "Use empty strings or nulls for fields if the information is not present or cannot be reasonably inferred."
        )
        
        user_prompt = f"Extract FHIR resource information from this text:\n'{text}'"
        
        messages = [
            ChatMessage(role="system", content=system_message),
            ChatMessage(role="user", content=user_prompt),
        ]
        
        try:
            output = self.llm.chat(messages)
            extracted_data = self._extract_json_from_llm_output(output.message.content)
            return extracted_data
        except Exception as e:
            print(f"Error in step 1 - information extraction: {str(e)}")
            print("Raw LLM output:", output.message.content)
            raise
    
    def step2_analyze_with_fhir_context(self, extracted_data: List[Dict[str, Any]]) -> str:
        """
        Step 2: Analyze extracted data using FHIR resource context.
        
        Args:
            extracted_data: Structured data from step 1
            
        Returns:
            Natural language analysis of the data with FHIR categorization
        """
        system_message = (
            "You are an expert AI assistant specializing in analyzing structured healthcare data. "
            "Your primary goal is to review the provided JSON data (extracted from an unstructured source) "
            "and categorize its content into meaningful, FHIR-relevant buckets. "
            "You MUST use the provided list of FHIR Resource Types and their descriptions as a precise guide "
            "to identify the most appropriate FHIR resource type(s) for the extracted information. "
            "For each categorized piece of information, describe what it represents, its value, "
            "and explicitly suggest the most suitable FHIR resource type (e.g., 'Organization', 'Location', 'Practitioner', etc.) "
            "and specific fields that would hold this information. "
            "Also, infer any potential relationships or 'edges' to other FHIR resources "
            "(e.g., an 'Organization' operates at a 'Location'). "
            "Do NOT generate JSON output. Instead, provide a clear, natural language summary or a bulleted list of your findings."
        )
        
        fhir_context = self._format_fhir_context()
        
        user_prompt = (
            "Here is the extracted information from Step 1:\n"
            f"```json\n{json.dumps(extracted_data, indent=2)}\n```\n\n"
            f"{fhir_context}"
            "Please analyze the extracted information and categorize it into meaningful buckets, "
            "explicitly mapping it to the all the relevant FHIR resource types and their key fields from the examples. "
            "Also, note any potential relationships or 'edges' between these categorized pieces of information "
            "that would be useful for a FHIR database."
        )
        
        messages = [
            ChatMessage(role="system", content=system_message),
            ChatMessage(role="user", content=user_prompt),
        ]
        
        try:
            output = self.llm.chat(messages)
            return output.message.content
        except Exception as e:
            print(f"Error in step 2 - FHIR context analysis: {str(e)}")
            raise
    
    def step3_identify_resources(self, extracted_data: List[Dict[str, Any]], analysis: str) -> List[Dict[str, Any]]:
        """
        Step 3: Identify core FHIR resources from the analysis.
        
        Args:
            extracted_data: Structured data from step 1
            analysis: Natural language analysis from step 2
            
        Returns:
            List of identified FHIR resources with relevant data and reasoning
        """
        system_message = (
            "You are an expert AI assistant specialized in converting categorized healthcare data "
            "into a structured JSON format that directly informs FHIR resource creation. "
            "You will be given two inputs: "
            "1. The initial JSON data extracted from an unstructured source (Step 1 output). "
            "2. A natural language summary/categorization of that data, informed by FHIR schemas (Step 2 output). "
            "Your task is to identify each major FHIR resource suggested in the Step 2 output "
            "(ignoring inferred relationships/edges for now) and provide the following for each: "
            "- **`fhir_resource_type`**: This key **MUST** contain the specific FHIR resource type "
            " (e.g., 'Organization', 'Location', 'Practitioner') identified by Step 2. "
            "- **`relevant_data`**: A JSON object containing the key-value pairs of data from the Step 1 output that are relevant to this FHIR resource. Only include fields that directly map to standard FHIR properties of this resource type. If a field from Step 1's output is not a direct FHIR property (e.g., a custom field), omit it. "
            "- **`reasoning`**: A concise explanation (1-2 sentences) of why this resource type is chosen and how the data maps. "
            "**CRITICAL**: Your output MUST be ONLY a JSON array, with absolutely no other text, commentary, "
            "markdown fences (```json), or other characters before or after the JSON. "
            "The JSON should be structured as: `[{'fhir_resource_type': '...', 'relevant_data': {...}, 'reasoning': '...'}]`."
        )
        
        user_prompt = (
            "Here is the initial extracted JSON data from Step 1:\n"
            f"```json\n{json.dumps(extracted_data, indent=2)}\n```\n\n"
            "Here is the natural language categorization and reasoning from Step 2:\n"
            f"```\n{analysis}\n```\n\n"
            "Please extract the core FHIR resources from these inputs, providing the resource type, "
            "the relevant data mapped from Step 1's JSON, and the reasoning for each, in a JSON array format. "
            "Remember, your response MUST be ONLY the JSON, and the resource type key MUST be `fhir_resource_type`."
        )
        
        messages = [
            ChatMessage(role="system", content=system_message),
            ChatMessage(role="user", content=user_prompt),
        ]
        
        try:
            output = self.llm.chat(messages)
            identified_resources = self._extract_json_from_llm_output(output.message.content)
            return identified_resources
        except Exception as e:
            print(f"Error in step 3 - resource identification: {str(e)}")
            print("Raw LLM output:", output.message.content)
            raise
    
    def step4_generate_fhir_json(self, identified_resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Step 4: Generate FHIR HL7 compliant JSON for each identified resource.
        
        Args:
            identified_resources: List of resources identified in step 3
            
        Returns:
            List of FHIR HL7 compliant JSON objects
        """
        final_outputs = []
        
        for resource_item in identified_resources:
            fhir_type = resource_item["fhir_resource_type"]
            
            # Get the relevant data and reasoning, with fallbacks
            relevant_data = resource_item.get("relevant_data", resource_item)
            reasoning = resource_item.get("reasoning", "No reasoning provided")
            
            # Find the schema for this FHIR resource type
            matching_schema_rows = self.schema_df[self.schema_df['name'].str.lower() == fhir_type.lower()]
            
            if matching_schema_rows.empty:
                print(f"Warning: No schema found for FHIR resource type '{fhir_type}'. Skipping.")
                continue
                
            schema_json = matching_schema_rows['schema'].iloc[0]
            
            system_message = (
                "You are an expert AI assistant specialized in generating FHIR HL7 compliant JSON. "
                "Your task is to produce a single FHIR resource JSON object based on the provided data and schema. "
                "Adhere strictly to the given FHIR JSON Schema. "
                "Ensure all data from 'relevant_data' is mapped to the correct FHIR fields. "
                "If a field from 'relevant_data' maps to a FHIR property that can accept null or an empty string/array, "
                "and the value is missing or empty, it is acceptable to include it as `null` or `[]` (empty array/object) if appropriate for the FHIR type. "
                "Do NOT include any extra fields that are not part of the FHIR schema. "
                "**CRITICAL**: Your output MUST be ONLY a single FHIR JSON object, with absolutely no other text, "
                "commentary, markdown fences (```json), or other characters before or after the JSON."
            )
            
            user_prompt = (
                f"Generate a FHIR {fhir_type} resource in JSON format.\n\n"
                "Here is the data relevant to this resource:\n"
                f"```json\n{json.dumps(relevant_data, indent=2)}\n```\n\n"
                "Here is the FHIR JSON Schema for a "
                f"{fhir_type} resource. Use this schema strictly for structure and data types:\n"
                f"```json\n{json.dumps(json.loads(schema_json), indent=2)}\n```\n\n"
                "Please provide ONLY the FHIR JSON. Remember that nulls and empty strings/arrays are acceptable for missing data where allowed by FHIR."
            )
            
            messages = [
                ChatMessage(role="system", content=system_message),
                ChatMessage(role="user", content=user_prompt),
            ]
            
            try:
                output = self.llm.chat(messages)
                fhir_json = self._extract_json_from_llm_output(output.message.content)
                final_outputs.append(fhir_json)
            except Exception as e:
                print(f"Error generating FHIR JSON for {fhir_type}: {str(e)}")
                print("Raw LLM output:", output.message.content)
        
        return final_outputs
    
    def transform(self, text: str) -> List[Dict[str, Any]]:
        """
        Transform unstructured text to FHIR HL7 resources.
        
        Args:
            text: Unstructured healthcare text to transform
            
        Returns:
            List of FHIR HL7 compliant JSON objects
        """
        print(f"Processing text: '{text[:100]}...'")
        
        # Step 1: Extract initial information
        print("\nStep 1: Extracting structured information...")
        extracted_data = self.step1_extract_information(text)
        
        # Step 2: Analyze with FHIR context
        print("\nStep 2: Analyzing with FHIR context...")
        analysis = self.step2_analyze_with_fhir_context(extracted_data)
        
        # Step 3: Identify resources
        print("\nStep 3: Identifying FHIR resources...")
        identified_resources = self.step3_identify_resources(extracted_data, analysis)
        
        # Step 4: Generate FHIR JSON
        print("\nStep 4: Generating FHIR HL7 JSON...")
        fhir_resources = self.step4_generate_fhir_json(identified_resources)
        
        print(f"\nTransformation complete. Generated {len(fhir_resources)} FHIR resources.")
        return fhir_resources


def process_text_to_fhir(
    spark: SparkSession, 
    text: str,
    model_name: str = "databricks-llama-4-maverick"
) -> List[Dict[str, Any]]:
    """
    Convenience function to process text to FHIR in one call.
    
    Args:
        spark: Active SparkSession
        text: Unstructured healthcare text to transform
        model_name: Databricks LLM model name to use
    
    Returns:
        List of FHIR HL7 compliant JSON objects
    """
    transformer = FHIRFly(spark, model_name=model_name)
    return transformer.transform(text)


# Example usage
if __name__ == "__main__":
    # This would be run when the script is executed directly
    from pyspark.sql import SparkSession
    
    # Sample text - replace with actual input in production
    sample_text = """
    Nevada Medical and Pain Institute in Las Vegas provides comprehensive pain management 
    services for patients throughout Nevada.
    """
    
    # Initialize SparkSession - In Databricks this is pre-initialized
    spark = SparkSession.builder.appName("FHIRfly").getOrCreate()
    
    # Process the text
    fhir_resources = process_text_to_fhir(spark, sample_text)
    
    # Display results
    print("\nGenerated FHIR Resources:")
    for i, resource in enumerate(fhir_resources):
        print(f"\nResource {i+1}:")
        pprint(resource)
