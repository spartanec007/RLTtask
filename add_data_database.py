from pymongo import MongoClient
import bson

# Подключение к MongoDB
client = MongoClient('localhost', 27017)  # Подставьте свои данные для подключения

# Выбор базы данных
db = client['RLTbase']  

# Выбор коллекции
collection = db['RLTbase']

# Загрузка данных из файла .bson
with open('sample_collection.bson', 'rb') as f:
    data = bson.decode_all(f.read())

# Вставка данных в коллекцию
collection.insert_many(data)

print("Данные успешно вставлены в коллекцию.")