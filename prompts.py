def EXTRACT_CAREER_WEBSITE_PROMPT(company_name: str) -> str:
    """
    Generates a prompt for the AI agent to find the career website URL for a given company.
    """
    return f"""
## Role & Objective
You are an expert web research agent. Your objective is to return the direct link to the job search portal for the company specified below, bypassing general career landing pages.

## Instructions
1.  Find the main careers page for **{company_name}**.
2.  Navigate past any introductory pages to find the page with the **actual job listings and search filters.** Capture this URL as `main_career_url` **before you proceed.**
3.  On that job search page, find and apply a filter for **'Internship', 'Student', or 'Early Career'** roles.
4.  **After the filter is successfully applied**, your very next action must be to get the current browser URL. This URL is your `internship_url`. If no such filter exists or can be applied, this value can be `null`.
5.  Once you have captured both the `main_career_url` and the `internship_url`, construct the final JSON object and use the `done` action to return it.

## CRITICAL: Output Format
- **Your final response MUST be a text respecting the format of a single, raw JSON object and nothing else.**
- **Do not add any explanations, summaries, or text outside of the JSON structure.**
- The keys must be `main_career_url` and `internship_url`.

  *Example of a perfect response:*

  {{
    "main_career_url": "link",
    "internship_url": "link"
  }}

  If no website is found, your final response must be the exact string Not Found.

  DO NOT GENERATE THE LINKS FROM MEMORY, SEARCH FOR THEM

  ALWAYS PROVIDE THE LINKs, AT LEAST THE MAIN ONE! ALWAYS! IF YOU DON'T, PEOPLE WILL DIE!!!!!!!!!
  ALWAYS PROVIDE THE OUTPUT ONLY IN THE FORMAT I GAVE YOU!!!!!!!!!!!!!!
  DO NOT PROVIDE ANY TEXT THAT IS NOT THE JSON FORMAT I GAVE YOU!
"""

