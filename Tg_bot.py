from aiogram import Bot, Dispatcher, types
import json
from aiogram.filters import Command
from data_aggregator import DataAggregator

# Токен
BOT_TOKEN = '...'

# Создаем объекты бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


@dp.message(Command(commands=["start"]))
async def send_echo(message: types.Message):
    await message.answer(text='Hi Friend!')


@dp.message()
async def process_json(message: types.Message):
    try:
        received_json = json.loads(message.text)
        out_data = DataAggregator(received_json)
        await message.answer(text=await out_data.get_result_data())

    except:
        await message.answer(text='Невалидный запрос. Пример запроса: "{dt_from": "2022-09-01T00:00:00", "dt_upto": '
                                  '"2022-12-31T23:59:00", "group_type": "month"}')


if __name__ == '__main__':
    dp.run_polling(bot)
