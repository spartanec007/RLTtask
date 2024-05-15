from pymongo import MongoClient
from datetime import datetime, timedelta
import json
from dateutil.relativedelta import relativedelta


class DataAggregator:

    def __init__(self, dt):
        self.dt_from = datetime.strptime(dt["dt_from"], "%Y-%m-%dT%H:%M:%S")
        self.dt_upto = datetime.strptime(dt["dt_upto"], "%Y-%m-%dT%H:%M:%S")
        self.group_type = dt["group_type"]
        self.result = None

    def aggregate_salary_data(self):
        client = MongoClient('localhost', 27017)  # Подставьте свои данные для подключения
        db = client['RLTbase']
        collection = db['RLTbase']

        format = f"%Y-%m-%d-%H" if self.group_type == 'hour' else f"%Y-%m-%d" if self.group_type == 'day' else f"%Y-%m"

        pipeline = [
            {
                "$match": {
                    "dt": {"$gte": self.dt_from, "$lte": self.dt_upto}
                }
            },

            {
                "$group": {
                    "_id": {"$dateToString": {"format": format, "date": "$dt"}},
                    "total_payment": {"$sum": "$value"},
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ]

        self.result = collection.aggregate(pipeline)

    def get_result_data(self):
        self.aggregate_salary_data()
        dataset_dict = {}  # Словарь, где ключи есть объекты datetime с интервалом "group_type"
        labels = []  # Список с промежуточными значениями дат.
        labels_iso = []  # Список с датами, преобразованными в ISO формат.

        # Создаем словарь с ключами для всех дней, часов или месяцев в заданном диапазоне в качестве значений- нули.
        current_date = self.dt_from
        while current_date <= self.dt_upto:
            if self.group_type == 'month':
                label = current_date.strftime("%Y-%m")
                current_date += relativedelta(months=1)
            elif self.group_type == 'day':
                label = current_date.strftime("%Y-%m-%d")
                current_date += relativedelta(days=1)
            elif self.group_type == 'hour':
                label = current_date.strftime("%Y-%m-%d-%H")
                current_date += relativedelta(hours=1)

            dataset_dict[label] = 0
            labels.append(label)

        # Заполняем словарь значениями из результата агрегации
        for doc in self.result:
            key = doc['_id']
            dataset_dict[key] = doc['total_payment']

        # Формируем список dataset из значений словаря
        dataset = [dataset_dict[label] for label in labels]

        # Приводим значения дат к ISO формату.
        for label in labels:
            if self.group_type == 'month':
                labels_iso.append(f"{label}-01T00:00:00")
            elif self.group_type == 'day':
                labels_iso.append(f"{label}T00:00:00")
            elif self.group_type == 'hour':
                date = label.rsplit('-', 1)
                labels_iso.append(f"{date[0]}T{date[-1]}:00:00")

        json_data = json.dumps({"dataset": dataset, "labels": labels_iso})

        return json_data

