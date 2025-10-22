import os
import json
import pymongo
from dotenv import load_dotenv
from bson.objectid import ObjectId
import sys

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Folder where the JSON results are stored
OUTPUT_FOLDER = "json_results" 

# Get MongoDB configuration from environment variables
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
# ---------------------

def check_config():
    """Validates that all necessary environment variables are set."""
    if not all([MONGO_URI, DB_NAME, COLLECTION_NAME]):
        print("Error: MONGO_URI, DB_NAME, and COLLECTION_NAME environment variables must be set.", file=sys.stderr)
        print("Please ensure your .env file is correct.", file=sys.stderr)
        return False
    return True

def update_companies_from_json():
    """
    Iterates through the JSON files in OUTPUT_FOLDER and updates
    MongoDB documents with the extracted career URLs.
    """
    if not check_config():
        return

    if not os.path.exists(OUTPUT_FOLDER):
        print(f"Error: Output folder '{OUTPUT_FOLDER}' not found.", file=sys.stderr)
        return

    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print(f"Successfully connected to MongoDB. (Database: {DB_NAME}, Collection: {COLLECTION_NAME})")
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}", file=sys.stderr)
        return

    success_count = 0
    error_count = 0
    files_processed = 0

    # Get a list of all JSON files in the directory
    json_files = [f for f in os.listdir(OUTPUT_FOLDER) if f.endswith(".json")]
    total_files = len(json_files)
    print(f"Found {total_files} JSON files to process in '{OUTPUT_FOLDER}'.")

    for filename in json_files:
        files_processed += 1
        file_path = os.path.join(OUTPUT_FOLDER, filename)
        
        # The filename (without .json) is the string representation of the ObjectId
        company_id_str = filename[:-5]

        try:
            # Convert the ID string back to a MongoDB ObjectId
            company_object_id = ObjectId(company_id_str)

            # Read and parse the JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            main_career_url = data.get("main_career_url")
            internship_url = data.get("internship_url")

            if not main_career_url:
                print(f"Warning: 'main_career_url' is missing or empty in {filename}. Skipping update for this field.")
                # We might still want to update the internship_url
            
            # Define the fields to update
            update_fields = {
                "$set": {
                    "career_website_url": main_career_url,
                    "internship_url": internship_url
                }
            }

            # Perform the update operation in MongoDB
            result = collection.update_one(
                {"_id": company_object_id},
                update_fields
            )

            if result.matched_count > 0:
                if result.modified_count > 0:
                    print(f"({files_processed}/{total_files}) Successfully updated company: {company_id_str}")
                else:
                    print(f"({files_processed}/{total_files}) Company {company_id_str} found, but data was already up-to-date.")
                success_count += 1
            else:
                print(f"({files_processed}/{total_files}) Error: Company with ID {company_id_str} not found in database.")
                error_count += 1

        except ObjectId.InvalidId:
            print(f"({files_processed}/{total_files}) Error: Invalid ObjectId from filename '{filename}'. Skipping.")
            error_count += 1
        except json.JSONDecodeError:
            print(f"({files_processed}/{total_files}) Error: Could not decode JSON from '{filename}'. Skipping.")
            error_count += 1
        except FileNotFoundError:
            print(f"({files_processed}/{total_files}) Error: File '{filename}' not found (should not happen). Skipping.")
            error_count += 1
        except Exception as e:
            print(f"({files_processed}/{total_files}) An unexpected error occurred for {filename}: {e}")
            error_count += 1

    # Close the MongoDB connection
    if client:
        client.close()
        print("\nMongoDB connection closed.")

    # --- Print Summary ---
    print("\n--- Update Complete ---")
    print(f"Total files processed: {files_processed}")
    print(f"Successful updates:  {success_count}")
    print(f"Failed updates/Errors: {error_count}")
    print("-----------------------")

if __name__ == '__main__':
    update_companies_from_json()