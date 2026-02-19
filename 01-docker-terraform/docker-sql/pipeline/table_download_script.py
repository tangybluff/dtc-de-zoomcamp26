import pandas as pd
from sqlalchemy import create_engine

# Download CSV; replace with your own URL if needed
url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
df = pd.read_csv(url)

# Connect to Postgres
engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')

# Load table
df.to_sql(name='taxi_zone_lookup', con=engine, if_exists='replace', index=False)

print("Ingestion complete.")