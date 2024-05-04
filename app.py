import csv
from datetime import datetime as dt
import pymysql
import asyncio
import aiohttp
import aiomysql

import pytz
from dotenv import dotenv_values

from tqdm import tqdm

import json

config_env = dotenv_values(".env")


async def get_connection():
    try:
        pool = await aiomysql.create_pool(
            host=config_env["DB_HOST"],
            user=config_env["DB_USER"],
            password=config_env["DB_PASSWORD"],
            db=config_env["DB_NAME"],
            port=int(config_env["DB_PORT"]),
        )
        return pool
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
    url = config_env["URL_TOKEN"]

    payload = json.dumps({
        "user": config_env["USER"],
        "password_hash": config_env["PASSWORD_HASH"],
        "hospital_code": config_env["HOSPITAL_CODE"]
    })
    headers = {
        'Content-Type': 'application/json'
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=payload) as response:
            data = await response.text()
            return data


async def main():
    # token = await get_token()
    pool = await get_connection()
    url_target = config_env["URL_TARGET"]
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT CID as cid FROM `t_data_target12`")
                result = await cur.fetchall()
                await request_to_moph(result, token)

    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        pool.close()
        await pool.wait_closed()


async def request_to_moph(result, token):
    url_target = config_env["URL_TARGET"]
    try:
        result = result[1:]
        for cid in tqdm(result):
            payload = json.dumps({
                "cid": cid[0]
            })
            headers = {
                'Content-Type': 'application/json'
            }
            print("cid0: ", cid[0])

            urls = f"{url_target}?cid={cid[0]}&hospital_code={config_env['HOSPITAL_CODE']}"
            print(urls)
            async with aiohttp.ClientSession() as session:
                headers = {
                    'Authorization': 'Bearer ' + token,
                    'Content-Type': 'application/json'
                }
                async with session.post(urls, headers=headers, data=payload) as response:
                    data = await response.json()
                    if len(data["result"]) != 0:
                        if len(data["result"]["person"]) != 0:
                            if len(data["result"]["vaccine_certificate"]) != 0:
                                print(data["result"]["vaccine_certificate"][0]["vaccination_list"])
                                vaccine_list = data["result"]["vaccine_certificate"][0]["vaccination_list"]

                                vac1 = vaccine_list[0]["vaccine_name"] if len(vaccine_list) >= 1 else ""
                                vac1_date = vaccine_list[0]["vaccine_date"] if len(vaccine_list) >= 1 else ""
                                vac2 = vaccine_list[1]["vaccine_name"] if len(vaccine_list) >= 2 else ""
                                vac2_date = vaccine_list[1]["vaccine_date"] if len(vaccine_list) >= 2 else ""
                                vac3 = vaccine_list[2]["vaccine_name"] if len(vaccine_list) >= 3 else ""
                                vac3_date = vaccine_list[2]["vaccine_date"] if len(vaccine_list) >= 3 else ""
                                vac4 = vaccine_list[3]["vaccine_name"] if len(vaccine_list) >= 4 else ""
                                vac4_date = vaccine_list[3]["vaccine_date"] if len(vaccine_list) >= 4 else ""
                                vac5 = vaccine_list[4]["vaccine_name"] if len(vaccine_list) >= 5 else ""
                                vac5_date = vaccine_list[4]["vaccine_date"] if len(vaccine_list) >= 5 else ""
                                vac6 = vaccine_list[5]["vaccine_name"] if len(vaccine_list) >= 6 else ""
                                vac6_date = vaccine_list[5]["vaccine_date"] if len(vaccine_list) >= 6 else ""

                            #     export all with column in first row
                            #     column = ["cid", "vac1", "vac1_date", "vac2", "vac2_date", "vac3", "vac3_date", "vac4", "vac4_date", "vac5", "vac5_date", "vac6", "vac6_date"]
                            #     with double quote
                                with open('data.csv', 'a', newline='', encoding='utf-8-sig') as file:

                                    # writer = csv.writer(file)
                                    writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL, quotechar='"')

                                    # writer.writerow(["cid", "vac1", "vac1_date", "vac2", "vac2_date"])
                                    writer.writerow([cid[0], vac1, vac1_date, vac2, vac2_date, vac3, vac3_date, vac4, vac4_date, vac5, vac5_date, vac6, vac6_date])



                            # update to database
                            # conn = await get_connection()
                            # try:
                            #     async with conn.cursor() as cur:
                            #         sql = "UPDATE t_data_target12 SET `status` = %s WHERE CID = %s"
                            #         await cur.execute(sql, ("1", cid[0]))
                            #         await conn.commit()
                            # except Exception as e:
                            #     print(f"Error in request_to_moph: {e}")
                            # finally:
                            #     conn.close()
                            #     await conn.wait_closed()


    except Exception as e:
        print(f"Error in request_to_moph: {e}")
    finally:
        print("Finish")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
