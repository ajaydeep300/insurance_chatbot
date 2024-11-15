from flask import Flask, request, jsonify, send_file
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import base64
import io
import re
import matplotlib.pyplot as plt
from groq import Groq

load_dotenv()

app = Flask(__name__)

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
        car_Type: type of car for example: SUV, sedan, Truck etc.
        car_color: the color of the crashed car.
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
    #print(sql_query)
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

'''
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
    




def query(question,res):
    
    sql_query = answer(question, res)
    print(sql_query)
    
    # Execute the generated SQL query
    result = execute_sql(sql_query)
    print(result)
    return result


'''







# Flask Routes
@app.route('/query', methods=['POST'])
def run_query():
    data = request.json
    question = data.get('question')
    res = data.get('res')
    
    sql_query = answer(question, res)
    result = execute_sql(sql_query)
    
    return jsonify({"query": sql_query, "result": result})

@app.route('/create_chart', methods=['POST'])
def create_chart():
    data = request.json
    chart_type = data.get('chart_type')
    result_data = data.get('result_data')

    img = None

    if chart_type == 'damage_distribution':
        img = create_damage_distribution_chart(result_data)
    elif chart_type == 'claim_amount_distribution':
        img = create_claim_amount_distribution_chart(result_data)
    elif chart_type == 'damage_cost_relationship':
        img = create_damage_cost_relationship_chart(result_data)
    elif chart_type == 'average_claim_by_car_type':
        img = create_average_claim_by_car_type_chart(result_data)

    if img:
        return send_file(img, mimetype='image/png')
    else:
        return jsonify({"error": "Invalid chart type or no data provided"}), 400







def create_damage_distribution_chart(data):
    # Define labels for each area corresponding to car parts
    labels = ["Front Bumper (AREA_1)", "Rear Bumper (AREA_2)", "Hood (AREA_3)",
              "Roof (AREA_4)", "Doors (AREA_5)", "Rear Panels (AREA_6)", "Wheels (AREA_7)"]
    
    # Extract average damage values
    values = list(data[0])  # data[0] contains averages for each area from SQL query

    # Plotting the bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(labels, values, color='skyblue')
    plt.xlabel("Car Parts")
    plt.ylabel("Average Damage Level (%)")
    plt.title("Damage Distribution by Car Part")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # Save the plot to an in-memory file
    img = io.BytesIO()
    plt.savefig(img, format='png')
    plt.close()
    img.seek(0)
    return img


def create_claim_amount_distribution_chart(data):
    # Extract claim amounts from the result
    claim_amounts = [row[0] for row in data]  # Assuming data is a list of tuples

    # Plotting the histogram
    plt.figure(figsize=(10, 6))
    plt.hist(claim_amounts, bins=10, color='skyblue', edgecolor='black')
    plt.xlabel("Claim Amount")
    plt.ylabel("Frequency")
    plt.title("Claim Amount Distribution for Similar Cars")
    plt.tight_layout()

    # Save the plot to an in-memory file
    img = io.BytesIO()
    plt.savefig(img, format='png')
    plt.close()
    img.seek(0)
    return img

def create_damage_cost_relationship_chart(data):
    # Extract mean damage and claim amounts from the result
    mean_damage = [row[0] for row in data]  # Assuming data is a list of tuples
    claim_amounts = [row[1] for row in data]

    # Plotting the scatter plot
    plt.figure(figsize=(10, 6))
    plt.scatter(mean_damage, claim_amounts, color='skyblue', edgecolor='black')
    plt.xlabel("Mean Damage Level (%)")
    plt.ylabel("Claim Amount ($)")
    plt.title("Damage Severity and Repair Cost Relationship")
    plt.grid(True)
    plt.tight_layout()

    # Save the plot to an in-memory file
    img = io.BytesIO()
    plt.savefig(img, format='png')
    plt.close()
    img.seek(0)
    return img

def create_average_claim_by_car_type_chart(data):
    # Extract car types and average claim amounts from the result
    car_types = ["Unknown" if row[0] is None else row[0] for row in data]
    avg_claim_amounts = [row[1] for row in data]

    # Plotting the bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(car_types, avg_claim_amounts, color='skyblue', edgecolor='black')
    plt.xlabel("Car Type")
    plt.ylabel("Average Claim Amount ($)")
    plt.title("Average Claim Amount by Car Type")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # Save the plot to an in-memory file
    img = io.BytesIO()
    plt.savefig(img, format='png')
    plt.close()
    img.seek(0)
    return img









'''

def display_claim_amount_distribution(res):
    # Query data for specified car make and model
    question = f"What are the claim amounts for this car_make, car_model and car year cars that match the damage report?"
    claim_data = query(question,res)
    
    if claim_data:
        # Generate and return the chart image
        chart_img = create_claim_amount_distribution_chart(claim_data)
        return chart_img
    else:
        print("No data available for specified car make and model.")

def display_damage_distribution(res):
    # Query data for specified car make and model
    question = "What is the average damage level for each car part for a car_make ,car_model and car_year inside the damage report?"
    damage_data = query(question,res)
    
    if damage_data:
        # Generate and return the chart image
        chart_img = create_damage_distribution_chart(damage_data)
        return chart_img
    else:
        print("No data available for specified car make and model.")

def display_damage_cost_relationship(res):
    # Query data for specified car make and model
    question = f"What are the mean damage levels and claim amounts for the same car_make, car_model and car year that match the damage report?"
    damage_cost_data = query(question,res)
    
    if damage_cost_data:
        # Generate and return the chart image
        chart_img = create_damage_cost_relationship_chart(damage_cost_data)
        return chart_img
    else:
        print("No data available for specified car make and model.")

def display_average_claim_by_car_type(res):
    # Query data for average claim amount by car type
    question = "What is the average claim amount for each car type?"
    avg_claim_data = query(question,res)
    
    if avg_claim_data:
        # Generate and return the chart image
        chart_img = create_average_claim_by_car_type_chart(avg_claim_data)
        return chart_img
    else:
        print("No data available for car types.")

'''


'''

chart1 = display_damage_distribution(res)
chart2 = display_claim_amount_distribution(res)
chart3 = display_damage_cost_relationship(res)
chart4 = display_average_claim_by_car_type(res)

if chart1:
    with open("damage_distribution.png", "wb") as f:
        f.write(chart1.getbuffer())


if chart2:
    with open("claim_amount_distribution.png", "wb") as f:
        f.write(chart2.getbuffer())


if chart3:
    with open("damage_cost_relationship.png", "wb") as f:
        f.write(chart3.getbuffer())


if chart4:
    with open("average_claim_by_car_type.png", "wb") as f:
        f.write(chart4.getbuffer())

        '''

if __name__ == '__main__':
    app.run(debug=True, port=5000)