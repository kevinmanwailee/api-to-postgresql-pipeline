import pandas as pd
import requests
import psycopg2

raw_data = requests.get("http://storage.googleapis.com/generall-shared-data/startups_demo.json")

with open("raw_files/raw_data.json", "w") as f:
    f.write(raw_data.text)

raw_df = pd.read_json("raw_files/raw_data.json",lines=True)

# filter links containing "https" and location in new york
# sort by name
secure_startups_df = raw_df.loc[(raw_df['link'].str.contains("https")) & (raw_df['city'] == "New York")].sort_values("name").reset_index(drop=True)
print(secure_startups_df)

conn = psycopg2.connect(
    database={db},
    host={host},
    user={user},
    password={pw},
    port={port}
)
cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS SECURE_STARTUPS (id bigserial Primary Key, name varchar(400), images varchar(400), alt varchar(1000), description varchar(4000), link varchar(400), city varchar(100))")

for row in secure_startups_df.itertuples():
    cur.execute("INSERT INTO SECURE_STARTUPS (NAME, IMAGES, ALT, DESCRIPTION, LINK, CITY) VALUES ('{}', '{}', '{}', '{}', '{}', '{}')".format(row.name.replace("'", "''"), row.images, row.alt.replace("'","''"), row.description.replace("'","''"), row.link, row.city))

conn.commit()
cur.close()
conn.close()
