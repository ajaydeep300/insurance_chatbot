#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import base64
from groq import Groq

load_dotenv()


import os
from groq import Groq
import base64


# Function to encode the image
def encode_image(image_path):
  with open(image_path, "rb") as image_file:
    return base64.b64encode(image_file.read()).decode('utf-8')

def damages(image):
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    # Path to your image
    image_path = image
    
    # Getting the base64 string
    base64_image = encode_image(image_path)
    
    user_prompt = """
        I have an image of a car. Please provide the following structured output based on the image:
            1. Car Make (Example: BMW)
            2. Car Model (Example: Model X)
            3. Car Year (Example: 2030)
            4. Provide damage percentages for the following **specific car parts ONLY**:
                - Front Bumper (corresponds to AREA_1)
                - Rear Bumper (corresponds to AREA_2)
                - Wheel (corresponds to AREA_3)
                - Windshield (corresponds to AREA_4)
                - Left Front Door (corresponds to AREA_5)
                - Right Front Door (corresponds to AREA_6)
                - Roof (corresponds to AREA_7)
        
        Your response **must strictly follow** the format below and **only include the car parts listed above**. Do not provide information for any parts not listed.
        
        EXAMPLE response format:
            1. Car Make: Tesla
            2. Car Model: Model S
            3. Car Year: 2021
            4. Car Part Damage Assessment:
                - Front Bumper (AREA_1): 85% damage
                - Rear Bumper (AREA_2): 70% damage
                - Wheel (AREA_3): 30% damage
                - Windshield (AREA_4): 40% damage
                - Left Front Door (AREA_5): 20% damage
                - Right Front Door (AREA_6): 15% damage
                - Roof (AREA_7): 80% damage
        
        Please only provide structured data and **no additional explanations**.
    """
    
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_prompt},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{base64_image}",
                        },
                    },
                ],
            }
        ],
        model="llava-v1.5-7b-4096-preview",
        temperature=0,
        max_tokens=1024,
        top_p=1,
        stream=False,
        stop=None,
    )
    
    response = chat_completion.choices[0].message.content
    print(response)
    return response

import re

def answer(question,res):
    client = Groq(
        api_key=os.environ.get("GROQ_API_KEY"),
    )
    schema_info = f"""
        You are a database agent. Your goal is to answer customer questions based on the historical accident data provided. The data is recorded in the following table schema: table name : claim

        claim_ID: Unique identifier for each insurance claim.
        client_name: Name of the client who filed the claim.
        policy_number: Policy number associated with the claim.
        assigned_adjuster: The adjuster assigned to handle the claim.
        claim_date: Date when the claim was filed.
        car_make: Make of the car involved in the accident.
        car_model: Model of the car.
        car_year: Year of the car.
        area_1 to area_7: Levels of damage to specific areas of the car. The areas are as follows:
        area_1: Front Bumper and Grille
        area_2: Rear Bumper
        area_3: Hood and Front Quarter Panels
        area_4: Roof and Windshield
        area_5: Doors and Side Panels
        area_6: Rear Quarter Panels and Trunk
        area_7: Wheels
        mean_damage: Average damage across all areas.
        claim_amount: The cost provided for fixing the car.
        crash_note: Notes about the crash for further context.
        You will act as an AI assistant that answers customer questions based on this table data. Customer questions will include queries such as:

        What is the estimated cost of fixing my car based on the given damage report?
        What is the average cost of fixing a car with the same make and model as mine?
        What is the cheapest or most expensive claim ever recorded?
        Which car models have the highest average claim costs?
        When generating SQL queries, make sure to:

        Use the columns effectively to find relevant records.
        For cost estimates, find similar claims by matching the car's make, model, year, and damage levels (area_1 to area_7) with a fault tolerance of +-10, then calculate the average claim_amount from similar records.
        Your response will be directly passed into a sql editor and thus your reponse will only include SQL query. You are strictly prohibited to use any markdowns or explainations.
        Generate appropriate SQL queries for general customer questions based on the data provided.
        Ensure your responses use simple and accurate SQL queries to produce the correct results.
        
        Use this damage report to generate SQL for repair cost estimation: {res}
    """
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": schema_info+ "Question: " + question,
            },
            {
                "role": "assistant",
                "content": "```SQL"
            }
        ],
        model="llama3-70b-8192",
        temperature=0,
        max_tokens=1024,
        top_p=1,
        stream=False,
        stop=None
    )
    ret = chat_completion.choices[0].message.content
    sql_query = re.sub(r"[`\n]+", " ", ret)
    # Ensure spaces around keywords by adding spaces where needed
    sql_query = re.sub(r"(FROM|WHERE|AND|SELECT)", r" \1 ", sql_query)
    sql_query = re.sub(r"\s{2,}", " ", sql_query)  # Replace multiple spaces with a single space
    print(sql_query)
    return sql_query



def execute_sql(sql_query):
    snowflake_conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    snowflake_cursor = snowflake_conn.cursor()
    try:
        snowflake_cursor.execute(sql_query)
        result = snowflake_cursor.fetchall()
    except snowflake.connector.Error as e:
        print(f"Error: {e}")
        result = None
    finally:
        snowflake_cursor.close()
        snowflake_conn.close()  # Close connection after cursor
    return result



def get_answer_from_llm(question, image):
    sql_query = answer(question,image) 
    result = execute_sql(sql_query)

    if result:
        return f"The answer is: {result}"
    else:
        return "No result found or an error occurred."


res = """   Car Make: Honda
            Car Model: Accord
            Car Year: 2020
            Damage Assessment:
                Front Bumper (AREA_1): 45% damage
                Rear Bumper (AREA_2): 50% damage
                Wheels (AREA_3): 75% damage
                Windshield (AREA_4): 25% damage
                Left Front Door (AREA_5): 10% damage
                Right Front Door (AREA_6): 65% damage
                Roof (AREA_7): 65% damage
"""
print(get_answer_from_llm("What is the total repair cost for all damaged parts of my car?",res))
print(get_answer_from_llm("Which car models by their year model have the highest average claim costs?",res))
print(get_answer_from_llm("What is the cheapest or most expensive claim ever recorded?",res))
print(get_answer_from_llm("What is the average cost of fixing a car with the same make and model as mine?",res))


