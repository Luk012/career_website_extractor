import os
import json
import random
from browser_use import Agent, Browser, ChatGoogle
from dotenv import load_dotenv
import asyncio
import prompts
import pymongo

load_dotenv()

OUTPUT_FOLDER = "json_results" 

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")


async def EXTRACT_CAREER_WEBSITE(company: dict, semaphore: asyncio.Semaphore) -> tuple:
    
    company_name = company['company_name']
    company_id = company['_id']

    async with semaphore:

        browser = None

        try:
            browser = Browser(headless= False) 
            prompt = prompts.EXTRACT_CAREER_WEBSITE_PROMPT(company_name)
            llm = ChatGoogle(model="gemini-flash-latest")

            agent = Agent(task=prompt, llm=llm, browser=browser)

            history = await agent.run()
        
            if history and history.is_successful():
                return (company_id, company_name, history.final_result())
            
            return (company_id, company_name, {"error": "Agent run was not successful or did not return a result."})
        
        except Exception as e:
            print(f"ERROR processing {company_name}: {e}")
            return (company_id, company_name, e)
        
        finally:
            if browser:
                await browser.kill()

def get_processed_ids(output_folder: str) -> set:
    if not os.path.exists(output_folder):
        return set()
    
    processed_ids = set()
    for filename in os.listdir(output_folder):
        if filename.endswith(".json"):
            processed_ids.add(filename[:-5]) 
    
    return processed_ids
            
def COMPANIES_TO_PROCESS() -> list[dict[str, any]]:
    
    processed_ids = get_processed_ids(OUTPUT_FOLDER)
    if processed_ids:
        print(f"found {len(processed_ids)} already processed companies")

    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    query = {
            "office_in_EU": True,
            "career_website_url": {"$in": [None, ""]}
        }
    
    projection = {"company_name": 1, "_id": 1}
    companies_cursor = collection.find(query, projection).limit(5000)
    
    companies_to_process = [
        company for company in companies_cursor 
        if str(company['_id']) not in processed_ids
    ]

    if client:
        client.close()
    
    print(f"{len(companies_to_process)} companies to process")
    return companies_to_process

async def main():
    companies = COMPANIES_TO_PROCESS()
    
    if not companies:
        print("no new companies to process")
        return

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    sem = asyncio.Semaphore(2) 

    tasks = [EXTRACT_CAREER_WEBSITE(company, sem) for company in companies]

    for future in asyncio.as_completed(tasks):
        company_id, company_name, result = await future

        company_id_str = str(company_id)

        if isinstance(result, Exception):
            continue

        if isinstance(result, str):
            base_filename = f"{company_id_str}.json"
            output_filename = os.path.join(OUTPUT_FOLDER, base_filename)
            try:
                json_output = json.loads(result)
                with open(output_filename, 'w', encoding='utf-8') as json_file:
                    json.dump(json_output, json_file, ensure_ascii=False, indent=4)
                print(f"Successfully saved results for '{company_name}'")
            except json.JSONDecodeError:
                print(f"failed to decode JSON for '{company_name}'")
        else:
            print(f"no valid result for '{company_name}'")
               
if __name__ == '__main__':
    asyncio.run(main())
