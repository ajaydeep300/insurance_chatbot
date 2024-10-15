#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
import base64
from groq import Groq

# Load environment variables from the .env file
load_dotenv()

# Establish MySQL connection using environment variables
mydb = mysql.connector.connect(
    host=os.getenv('MYSQL_HOST'),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    database=os.getenv('MYSQL_DATABASE')
)
cursor = mydb.cursor()

cursor.execute("select * from accounts")

results = cursor.fetchall();
for r in results:
    print(r)



cursor.execute("SELECT * FROM car_parts")
results = cursor.fetchall()
for r in results:
    print(r)

# Commit and close the connection
mydb.commit()
cursor.close()
mydb.close()


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
            1. Car Make (Example: Tesla)
            2. Car Model (Example: Model S)
            3. Car Year (Example: 2021)
            4. Provide damage percentages for the **following car parts ONLY**:
                - Front Bumper
                - Left Fender
                - Hood
                - Left Headlight
                - Wheel
                - Windshield
                - Rear Bumper
                - Battery Pack
                - Charge Port
                - Side Mirror (Left)
                - Side Mirror (Right)
        
        Your response **must strictly follow** the format below and **only include the car parts listed above**. Do not provide information for any parts not listed.
        
        EXAMPLE response format:
            1. Car Make: Tesla
            2. Car Model: Model S
            3. Car Year: 2021
            4. car_part Damage Assessment:
                - Front Bumper: 85% damage
                - Left Fender: 70% damage
                - Hood: 30% damage
                - Left Headlight: 40% damage
                - Wheel: 20% damage
                - Windshield: 15% damage
                - Rear Bumper: 80% damage
                - Battery Pack: 50% damage
                - Charge Port: 25% damage
                - Side Mirror (Left): 60% damage
                - Side Mirror (Right): 40% damage
        
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


def answer(question,res):
    client = Groq(
        api_key=os.environ.get("GROQ_API_KEY"),
    )
    schema_info = f"""
    You have access to the following database schema:
    
    Table 'cars':
        - car_id (INT, PRIMARY KEY): Unique identifier for each car.
        - make (VARCHAR): Manufacturer of the car (e.g., 'Tesla').
        - model (VARCHAR): Model of the car (e.g., 'Model S').
        - year (INT): Year the car was manufactured.
    
    Table 'car_parts':
        - part_id (INT, PRIMARY KEY): Unique identifier for each car part.
        - car_id (INT, FOREIGN KEY REFERENCES cars(car_id)): Foreign key linking the part to the specific car.
        - part_name (VARCHAR): Name of the car part (e.g., 'Front Bumper', 'Battery Pack').
        - part_type (VARCHAR): Type of car part (e.g., 'body', 'mechanical', 'electrical').
        - repair_cost (DECIMAL): Cost to repair the car part.
        - replacement_cost (DECIMAL): Cost to replace the car part.
        - description (TEXT): Additional details about the part (e.g., 'The front bumper is designed to absorb impacts.').
    
    The 'cars' table stores information about each car, while the 'car_parts' table stores information about individual car parts, including their repair and replacement costs.
    
    ### Instructions for SQL Generation:
    
    - You are **only** required to generate a single SQL query based on the user's request. **Do not generate any explanations, comments, or additional text.**
    - The query must be properly formatted and executable in a MySQL database.
    - Use the following rules when generating the query:
    
    #### Repair Cost Estimate (Specific Instructions):
    1. If an **estimated repair cost** is requested: 
       - Use the **repair_cost** from the `car_parts` table if the damage percentage to the part is **50% or less**.
       - Use the **replacement_cost** if the damage to the part is **greater than 50%**.
       - To access the information on car part damage percentage, reference : {res}
    2. If the user asks for a **minimum estimate** for repair, return the **minimum repair cost** for all car parts.
    3. Finally, **sum** the selected repair and replacement costs to provide a total estimate.
    
    #### General Questions:
    - For all other general questions, simply return an SQL query that addresses the user's request based on the provided schema and previous chat responses focus, on the report generated and use only the values sepcified in the report.
    Note that you can only answer in SQL Queries
    
    ### Example Query Format:
    if response give the following damage report : 
        1. Car Make: Tesla
        2. Car Model: Model S
        3. Car Year: 2021
        4. Car Part Damage Assessment:
                       - Front Bumper: 100% damage
                       - Left Fender: 100% damage
                       - Hood: 30% damage
                       - Left Headlight: 100% damage
                       - Wheel: 100% damage
                       - Windshield: 100% damage
                       - Rear Bumper: 100% damage
                       - Battery Pack: 80% damage
                       - Charge Port: 20% damage
                       - Side Mirror (Left): 50% damage
                       - Side Mirror (Right): 20% damage
    ```create sqls like these where you access the damage percentage from the report along with the car, make, model,year and simply return the cost
    SELECT SUM(
      CASE 
        WHEN part_name = 'Front Bumper' AND 100 > 50 THEN replacement_cost
        WHEN part_name = 'Left Fender' AND 100 > 50 THEN replacement_cost
        WHEN part_name = 'Hood' AND 30 <= 50 THEN repair_cost
        WHEN part_name = 'Left Headlight' AND 100 > 50 THEN replacement_cost
        WHEN part_name = 'Wheel' AND 100 > 50 THEN replacement_cost
        WHEN part_name = 'Windshield' AND 100 > 50 THEN replacement_cost
        WHEN part_name = 'Rear Bumper' AND 100 > 50 THEN replacement_cost
        WHEN part_name = 'Battery Pack' AND 80 > 50 THEN replacement_cost
        WHEN part_name = 'Charge Port' AND 20 <= 50 THEN repair_cost
        WHEN part_name = 'Side Mirror (Left)' AND 50 <= 50 THEN repair_cost
        WHEN part_name = 'Side Mirror (Right)' AND 20 <= 50 THEN repair_cost
        ELSE 0
      END
    ) AS total_cost_to_fix
    FROM car_parts
    WHERE car_id = (SELECT car_id FROM cars WHERE make = 'Tesla' AND model = 'Model S' AND year = 2021);

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
    print(ret)
    return ret



def execute_sql(sql_query):
    connection = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE')
    )

    cursor = connection.cursor();
    try:
        cursor.execute(sql_query)
        result = cursor.fetchall()
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        result = None
    finally:
        cursor.close()
        connection.close()
    return result


def get_answer_from_llm(question, image):
    sql_query = answer(question,image) 
    result = execute_sql(sql_query)

    if result:
        return f"The answer is: {result}"
    else:
        return "No result found or an error occurred."



image = "car.jpeg"
res = damages(image)
print(get_answer_from_llm("What is the total repair cost for all damaged parts of my car?",res))
print(get_answer_from_llm("Which part of the car has the highest replacement cost?",res))
print(get_answer_from_llm("How many body parts of the car are damaged?",res))
print(get_answer_from_llm("What is the total repair cost for the car if we only consider parts with 50% or less damage?",res))
print(get_answer_from_llm("What is the average cost to repair all parts of the car with more than 50% damage?",res))





