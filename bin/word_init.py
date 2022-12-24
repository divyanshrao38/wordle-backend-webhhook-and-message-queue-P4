# Imports
import json
import asyncio
import databases
import toml
from quart import Quart
from quart_schema import QuartSchema

# Initialize app
app = Quart(__name__)
QuartSchema(app)
app.config.from_file(f"../etc/wordle.toml", toml.load)


# Load json from file and convert unicode to string.
def load_json_from_file(file_name):
    f = open(file_name)
    data = json.load(f)
    values = []
    # converting unicode to string values
    for item in data:
        values.append(str(item))
    return values


# Establish database connection.
async def _get_db():
    db = databases.Database(app.config["DATABASES"]["PRIMARY_GAME_URL"])
    await db.connect()
    return db


# Populate valid and correct words into database.
async def load_data(file_name, table_name):
    data = load_json_from_file(file_name)
    words = []
    for item in data:
        words.append({"word": item})

    print("Loading data into " + table_name + " table, please wait...")
    db = await _get_db()
    await db.execute_many("INSERT into " + table_name + "(" + table_name[:-1] + ") values(:word)", words)

# Run when executed as script.
if __name__ == "__main__":
    asyncio.run(load_data('./share/correct.json', 'correct_words'))
    asyncio.run(load_data('./share/valid.json', 'valid_words'))
    print("Loading of tables complete")
