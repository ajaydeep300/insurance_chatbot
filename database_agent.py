#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

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


# In[2]:


df = pd.read_csv('churn_data.csv')

create_table = """CREATE TABLE IF NOT EXISTS churn (
    customer_id VARCHAR(50) PRIMARY KEY,
    gender VARCHAR(10),
    customer_age INT,
    marital_status VARCHAR(20),
    occupation VARCHAR(50),
    location VARCHAR(50),
    customer_credit_score INT,
    customer_total_transactions INT,
    customer_total_cash_balance DECIMAL(15, 2),
    customer_total_cards INT,                    -- Total number of cards
    customer_total_accounts INT,                 -- Total number of accounts
    customer_total_debt DECIMAL(15, 2),          -- Total debt or outstanding balance
    customer_total_loans INT,                    -- Total number of loans
    customer_total_savings_accounts INT,         -- Total number of savings accounts
    customer_total_checking_accounts INT,        -- Total number of checking accounts
    customer_total_credit_accounts INT,          -- Total number of credit accounts
    customer_total_mortgage_accounts INT,        -- Total number of mortgage accounts
    customer_account_utilization_rate DECIMAL(15, 2), -- Utilization rate (credit utilization)
    customer_long_term_savings DECIMAL(15, 2),   -- Long-term savings balance
    customer_short_term_savings DECIMAL(15, 2),  -- Short-term savings balance
    customer_active_credit_cards INT,            -- Number of active credit cards
    customer_overdue_payments INT                -- Number of overdue payments
)"""

cursor.execute(create_table)

for index, row in df.iterrows():
    insert_query = """INSERT IGNORE INTO churn (customer_id, gender, customer_age, marital_status, occupation, location, customer_credit_score, 
    customer_total_transactions, customer_total_cash_balance, customer_total_cards, customer_total_accounts, 
    customer_total_debt, customer_total_loans, customer_total_savings_accounts, customer_total_checking_accounts, 
    customer_total_credit_accounts, customer_total_mortgage_accounts, customer_account_utilization_rate, 
    customer_long_term_savings, customer_short_term_savings, customer_active_credit_cards, customer_overdue_payments)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = tuple(row)
    cursor.execute(insert_query, values)

cursor.execute("select * from churn")
results = cursor.fetchall()
for r in results:
    print(r)
mydb.commit()
cursor.close()
mydb.close()


# In[3]:


from groq import Groq

schema_info = """
You have access to the following database schema:
Table 'churn': 
    customer_id (VARCHAR),
    gender (VARCHAR),
    customer_age (INT),
    marital_status (VARCHAR),
    occupation (VARCHAR),
    location (VARCHAR),
    customer_credit_score (INT),
    customer_total_transactions (INT),
    customer_total_cash_balance (DECIMAL),
    customer_total_cards (INT),
    customer_total_accounts (INT),
    customer_total_debt (DECIMAL),
    customer_total_loans (INT),
    customer_total_savings_accounts (INT),
    customer_total_checking_accounts (INT),
    customer_total_credit_accounts (INT),
    customer_total_mortgage_accounts (INT),
    customer_account_utilization_rate (DECIMAL),
    customer_long_term_savings (DECIMAL),
    customer_short_term_savings (DECIMAL),
    customer_active_credit_cards (INT),
    customer_overdue_payments (INT)

Only return a **single SQL query** that answers the question. Do not include any explanations, additional text, or comments. The query should be formatted properly and valid for execution in a MySQL database.
"""

client = Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)

def answer(question):
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
        model="llama3-8b-8192",
    )
    ret = chat_completion.choices[0].message.content
    print(ret)
    return ret


# In[4]:


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
    except mysql.connector.Eroor as e:
        print(f"Error: {e}")
        result = None
    finally:
        cursor.close()
        connection.close()
    return result
    


# In[5]:


def get_answer_from_llm(question):
    sql_query = answer(question) 
    result = execute_sql(sql_query)

    if result:
        return f"The answer is: {result}"
    else:
        return "No result found or an error occurred."


# In[6]:


print(get_answer_from_llm("How many rows of data are there in the table?"))
print(get_answer_from_llm("How many customers have a credit score above 700?"))
print(get_answer_from_llm("What is the average cash balance of customers who have more than 2 credit accounts and fewer than 3 overdue payments?"))
print(get_answer_from_llm("List the top 5 locations with the highest average customer debt, showing the location and average debt."))
print(get_answer_from_llm("For customers with more than 2 active credit cards, what is the total number of transactions made by customers with a credit score below 600?"))
print(get_answer_from_llm("how many people that are married have more than 2 active credit/debit cards"))


# In[ ]:




