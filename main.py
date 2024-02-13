from datetime import datetime as dt
import pymysql
import asyncio
import aiohttp
import aiomysql

import pytz
from dotenv import dotenv_values

from tqdm import tqdm

import json

env = dotenv_values(".env")


async def get_connection():
    try:
        return await aiomysql.connect(
            host=env["DB_HOST"],
            user=env["DB_USER"],
            password=env["DB_PASSWORD"],
            db=env["DB_NAME"],
            port=int(env["DB_PORT"]),
        )
    except Exception as e:
        print(f"Error in get_connection: {e}")
        return None


def thai_datetime():
    # datetime thailand
    time = dt.now()
    tz = pytz.timezone('Asia/Bangkok')
    thai_time = time.astimezone(tz)
    datetime_now = thai_time.strftime('%Y-%m-%d %H:%M:%S')
    return datetime_now


async def get_token():
    url = env["URL_TOKEN"]

    payload = json.dumps({
        "user": env["USER"],
        "password_hash": env["PASSWORD_HASH"],
        "hospital_code": env["HOSPITAL_CODE"]
    })
    headers = {
        'Content-Type': 'application/json'
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=payload) as response:
            data = await response.text()
            return data


async def get_data_vaccine(token):
    url_target = env["URL_IMMUNIZATION"]

    try:
        connection = await get_connection()
        async with connection.cursor() as cursor:
            sql = "SELECT CID as cid FROM `t_data_target12`"
            await cursor.execute(sql)
            result = await cursor.fetchall()

            for row in result:
                urls = f"{url_target}?cid={row[0]}&hospital_code={env['HOSPITAL_CODE']}"
                print(urls)
                # Continue with your logic for processing vaccine data

    except Exception as e:
        print(f"Error in get_data_vaccine: {e}")


async def main():
    token = await get_token()
    if token:
        await get_data_vaccine(token)


asyncio.run(main())
