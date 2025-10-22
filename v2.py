import os
import json
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
            browser = Browser(headless=True) 
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
            
def COMPANIES_TO_PROCESS() -> list[dict[str, any]]:
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    query = {
            "office_in_EU": True,
            "career_website_url": {"$in": [None, ""]}
        }
    
    projection = {"company_name": 1, "_id": 1}
    companies_cursor = collection.find(query, projection).limit(1000)
    companies_to_process = list(companies_cursor)

    if client:
        client.close()

    return companies_to_process

async def main():
    companies = COMPANIES_TO_PROCESS()
    
    if not companies:
        return

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    sem = asyncio.Semaphore(5) 

    tasks = [EXTRACT_CAREER_WEBSITE(company, sem) for company in companies]

    for future in asyncio.as_completed(tasks):
        company_id, company_name, result = await future

        if isinstance(result, Exception):
            continue

        if isinstance(result, str):
            base_filename = f"{str(company_id)}.json"
            output_filename = os.path.join(OUTPUT_FOLDER, base_filename)
            try:
                json_output = json.loads(result)
                with open(output_filename, 'w', encoding='utf-8') as json_file:
                    json.dump(json_output, json_file, ensure_ascii=False, indent=4)
            except json.JSONDecodeError:
                print(f"failed to decode JSON for '{company_name}'")
        else:
            print(f"no valid result for '{company_name}'")
               
if __name__ == '__main__':
    asyncio.run(main())