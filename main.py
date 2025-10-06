import os
import json 
from browser_use import Agent, ChatGoogle
from dotenv import load_dotenv
import asyncio
import prompts
import pymongo

load_dotenv()

OUTPUT_FOLDER = "json_results" 

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")


async def EXTRACT_CAREER_WEBSITE(company_name: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        print(f"Starting processing for: {company_name}")
        prompt = prompts.EXTARCT_CAREER_WEBSITE_PROMPT(company_name)
        llm = ChatGoogle(model="gemini-flash-latest")

        agent = Agent(task=prompt, llm=llm)

        history = await agent.run()
        print(f"Finished processing for: {company_name}")
        if history and history.is_successful():
            return history.final_result()


def COMPANIES_TO_PROCESS() -> list[dict[str, any]]:
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    query = {
            "office_in_EU": True,
            "career_website_url": {"$in": [None, ""]}
        }
    
    projection = {"company_name": 1, "_id": 1}
    companies_cursor = collection.find(query, projection)
    companies_to_process = list(companies_cursor)

    if client:
        client.close()

    return companies_to_process

async def main():
    companies = COMPANIES_TO_PROCESS()

    sem = asyncio.Semaphore(3)

    tasks = [EXTRACT_CAREER_WEBSITE(company['company_name'], sem) for company in companies]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for company_doc, output_string in zip(companies, results):
        company_id = company_doc['_id']
        if isinstance(output_string, Exception):
            print(f"Task for company ID {company_id} failed: {output_string}")
            continue

        if output_string:
            if not os.path.exists(OUTPUT_FOLDER):
                os.makedirs(OUTPUT_FOLDER)
                
            base_filename = f"{str(company_id)}.json"
            output_filename = os.path.join(OUTPUT_FOLDER, base_filename)
            json_output = json.loads(output_string)
            with open(output_filename, 'w', encoding='utf-8') as json_file:
                json.dump(json_output, json_file, ensure_ascii=False, indent= 4)
               
if __name__ == '__main__':
    asyncio.run(main())