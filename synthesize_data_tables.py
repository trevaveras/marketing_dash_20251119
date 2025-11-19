

#!pip install faker
import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import duckdb
import numpy as np




# create customers table

fake = Faker()

industries = [
    'Technology', 'Healthcare', 'Finance', 'Retail', 'Manufacturing',
    'Education', 'Hospitality', 'Energy', 'Transportation', 'Entertainment'
]

num_records = 10000
data = []

for i in range(num_records):
    company_name = fake.company()
    industry = random.choice(industries)
    employee_count = random.randint(10, 10000)
    customer_id = f"CUST{100000 + i}"
    
    data.append({
        'company_name': company_name,
        'industry': industry,
        'employee_count': employee_count,
        'customer_id': customer_id
    })

customers = pd.DataFrame(data)


# create activations table

fake = Faker()

customer_ids = customers['customer_id'].tolist()

campaign_types = ['Retargeting', 'Website Traffic', 'Awareness']
sales_modes = ['Inbound', 'Outbound', 'Self-Service']

activation_data = []

for cust_id in customer_ids:
    # Random date in the past 3 years
    first_spend = fake.date_between(start_date='-2y', end_date='-30d')
    last_spend = fake.date_between(start_date=first_spend, end_date='today')

    record = {
        'customer_id': cust_id,
        'customer_first_spend_date': first_spend,
        'last_spend_date': last_spend,
        'daily_revenue': round(random.uniform(50, 5000), 2),  # random revenue
        'product_campaign_type': random.choice(campaign_types),
        'sales_mode': random.choice(sales_modes)
    }

    activation_data.append(record)

activations = pd.DataFrame(activation_data)


#create the sales_activities table


fake = Faker()
customer_ids = customers['customer_id'].tolist()
num_customers = len(customer_ids)
target_total = 50000

sales_activities1 = []
for cust_id in customer_ids:
    sales_activities1.append({
        'customer_id': cust_id,
        'activity_date': fake.date_between(start_date='-2y', end_date='today'),
        'interaction': random.choice(['email', 'call', 'meeting'])
    })

remaining = target_total - num_customers
customer_weights = [random.randint(1, 5) for _ in customer_ids]

for _ in range(remaining):
    cust_id = random.choices(customer_ids, weights=customer_weights)[0]
    sales_activities1.append({
        'customer_id': cust_id,
        'activity_date': fake.date_between(start_date='-2y', end_date='today'),
        'interaction': random.choice(['email', 'call', 'meeting'])
    })

sales_activities = pd.DataFrame(sales_activities1)


#create sales_reps table

fake = Faker()

customer_ids = customers['customer_id'].unique().tolist()
assert len(customer_ids) == 10000

rep_names = set()
while len(rep_names) < 750:
    rep_names.add(fake.unique.name())  # `unique` ensures no duplicates

rep_list = []
for name in rep_names:
    rep_list.append({
        'ae_name': name,
        'team': random.choice(['inbound', 'outbound'])
    })

reps_df = pd.DataFrame(rep_list)

assigned_reps = random.choices(reps_df['ae_name'].tolist(), k=len(customer_ids))
assignment_df = pd.DataFrame({
    'customer_id': customer_ids,
    'ae_name': assigned_reps
})

sales_reps = assignment_df.merge(reps_df, on='ae_name', how='left')


#set the dfs into tables

duckdb.register("customers", customers)
duckdb.register("activations", activations)
duckdb.register("sales_activities", sales_activities)
duckdb.register("sales_rep_assignments", sales_reps)