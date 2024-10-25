import pandas as pd
from faker import Faker
import random
import numpy as np
from datetime import datetime
from openai import OpenAI


# Specify size parameters
n_names = 35


# Setup faker for Dutch names and data
fake = Faker('nl_NL')
Faker.seed(0)
random.seed(0)

# Generate client names
names = [fake.name() for _ in range(n_names)]
client_avg_lengths = {name: min(max(20, int(np.random.normal(70, 30))), 250) for name in names}
client_sd_lengths = {name:abs(np.random.normal(0, 0.3) * client_avg_lengths[name]) for name in names}

# Each name appears a number of times drawn from a distribution
client_names = [name for name in names for _ in range(max(1, np.random.poisson(10**np.random.choice([1])*3)))]


# Generate random timestamps starting from 2020
random_dates = pd.to_datetime(
    [fake.date_time_between(start_date='-3y', end_date='now') for _ in range(len(client_names))]
).sort_values().strftime('%Y-%m-%d %H:%M')

# Randomly assign zorgverlener IDs
zorgverlener_ids = [f"ZV10{random.randint(1,17)}" for _ in range(len(random_dates))]

# Creating the DataFrame
data = {
    "Cliëntnaam": client_names,
    "Tijdstip": random_dates,
    "Zorgverlener ID": zorgverlener_ids
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Shuffle client names to ensure they are mixed up and then sort by 'Tijdstip'
#random.shuffle(df['Cliëntnaam'])
#df_sorted = df.sort_values(by='Tijdstip')

df_sorted = df.sort_values(by=["Cliëntnaam", "Tijdstip"])


client = OpenAI(
    # This is the default and can be omitted
    api_key = ""
)

# Assuming you have a specific entity and sector to check

def rapport_generator(name):
    length = int(np.random.normal(client_avg_lengths[name], client_sd_lengths[name]))
    response = client.chat.completions.create(
        messages=[
        {"role": "system", "content": f"Je bent een generator voor teksten. Maak de tekst ongeveer {length} woorden lang. Het moet zijn alsof een persoonlijk begeleider een dagelijkse rapportage schrijft over iemand met een verstandelijke beperking. Zorg er voor dat er nepnamen, nep adressen, nep e-mail adressen en nep telefoon nummers in de rapportage voorkomen. Het stukje tekst moet gaan over de dag van de persoon met de verstandelijke beperking. "},
        {"role": "user", "content": name}
        ],
        max_tokens = 300,
        model="gpt-3.5-turbo",
    )
    result = response.choices[0].message.content
    print(name)
    return result



df_sorted['rapport'] = df_sorted['Cliëntnaam'].apply(rapport_generator)

# Gebruik datetime om de huidige datum en tijd te verkrijgen en formatteren
current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
# Maak een pad voor het opslaan van het bestand, inclusief de huidige datum en tijd
csv_path = f'ECD_dummy_dataset_{current_time}.csv'
# Exporteer het DataFrame naar een CSV-bestand zonder index
df_sorted.to_csv(csv_path, index=False)
# Print het pad waar het bestand is opgeslagen
print(f'Bestand opgeslagen op: {csv_path}')