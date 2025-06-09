import pkgutil
import inspect
import importlib
from fhir.resources import FHIRAbstractModel
# from library import get_all_fhir_resource_info_with_descriptions
def get_all_fhir_resource_info_with_descriptions():
    """
    Retrieves information for all FHIR resource and complex data types,
    including their name (resource type) and a short description.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary has:
                    - 'name': The name of the FHIR resource/type (e.g., 'Patient', 'HumanName').
                    - 'description': The first line of its docstring, representing a short description.
    """
    resource_info_list = []
    
    # Get the list of all FHIR resource submodules (e.g., 'patient', 'organization')
    # This is the same logic as your previous successful output
    submodules_to_check = []
    for importer, modname, ispkg in pkgutil.walk_packages(fhir.resources.__path__):
        if not ispkg and \
           not modname.startswith('fhirabstractmodel') and \
           not modname.startswith('__init__') and \
           not modname.startswith('fhirtypes'):
            submodules_to_check.append(modname)

    # Now, for each submodule, dynamically import the class and get its description
    for submodule_name in sorted(list(set(submodules_to_check))):
        try:
            # Dynamically import the module, e.g., 'fhir.resources.patient'
            full_module_name = f"fhir.resources.{submodule_name}"
            module = importlib.import_module(full_module_name)

            # The class name is typically the same as the submodule name, but capitalized
            # E.g., 'patient' module contains 'Patient' class
            class_name = submodule_name.capitalize()
            
            # Special cases where module name != class name or multiple classes exist
            if submodule_name == 'list': # 'list' is a Python keyword, so it's 'List'
                class_name = 'List'
            elif submodule_name == 'codesystem':
                class_name = 'CodeSystem'
            elif submodule_name == 'valueset':
                class_name = 'ValueSet'
            elif submodule_name == 'searchparameter':
                class_name = 'SearchParameter'
            # Add other special cases if you encounter them.
            # Generally, it's just `capitalize()`.

            if hasattr(module, class_name):
                fhir_class = getattr(module, class_name)

                # Ensure it's a class and an actual FHIR resource/type
                if inspect.isclass(fhir_class) and issubclass(fhir_class, FHIRAbstractModel) and \
                   fhir_class.__name__ not in ('FHIRAbstractModel', 'FHIRAbstractType'):
                    
                    description = inspect.getdoc(fhir_class)
                    if description:
                        # Joins all lines, replacing newlines with spaces.
                        short_description = " ".join(line.strip() for line in description.split('\n')[3:] if line.strip())
                    else:
                        short_description = "No description available."
                    remove_str = "Description: Disclaimer: Any field name ends with ``__ext`` doesn't part of Resource StructureDefinition, instead used to enable Extensibility feature for FHIR Primitive Data Types."
                    #remove from short description
                    short_description = short_description.replace(remove_str, "")
                    resource_info_list.append({
                        "fhir_resource_name": fhir_class.__name__,
                        "fhir_resource_description": short_description
                    })
        except ImportError as e:
            print(f"Could not import {full_module_name}: {e}")
        except AttributeError:
            # This can happen if class_name doesn't exist in the module
            # or if the module doesn't contain a direct FHIR resource class
            # print(f"Warning: Could not find class '{class_name}' in module '{full_module_name}'")
            pass
        except Exception as e:
            print(f"An unexpected error occurred for {submodule_name}: {e}")

    # Sort the final list by name
    resource_info_list.sort(key=lambda x: x['fhir_resource_name'])
    return resource_info_list

# for k in get_all_fhir_resource_info_with_descriptions():
#     print()
#     print("Resource Name: ", k["fhir_resource_name"])
#     print("Resource Description: ", k["fhir_resource_description"])

