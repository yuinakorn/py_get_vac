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

# token = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJBZG1pbl8wMDAzN0AwMDAzNyIsImlhdCI6MTcwNTY1NjczNSwiZXhwIjoxNzA1NzM5NTM1LCJpc3MiOiJNT1BIIEFjY291bnQgQ2VudGVyIiwiYXVkIjoiTU9QSCBBUEkiLCJjbGllbnQiOnsidXNlcl9pZCI6NTI5LCJ1c2VyX2hhc2giOiIyOUYwRDNFNjQ4OUUzRkIyQUY0OUFDNkIyRTE5RTIxMTdFNDU4RUY0RUVFRDIwQkU0NEMxM0QxODNERTFFMDBEOEFDQUZGIiwibG9naW4iOiJBZG1pbl8wMDAzNyIsIm5hbWUiOiLguJnguLLguKLguJnguITguKMg4LiY4Lij4Lij4Lih4LmC4LiX4LmK4LiwIiwiaG9zcGl0YWxfbmFtZSI6IuC4quC4s-C4meC4seC4geC4h-C4suC4meC4quC4suC4mOC4suC4o-C4k-C4quC4uOC4guC4iOC4seC4h-C4q-C4p-C4seC4lOC5gOC4iuC4teC4ouC4h-C5g-C4q-C4oeC5iCIsImhvc3BpdGFsX2NvZGUiOiIwMDAzNyIsImVtYWlsIjoieWl1bmFrb3JuQGhvdG1haWwuY29tIiwiYWNjb3VudF9hY3RpdmF0ZWQiOnRydWUsImFjY291bnRfc3VzcGVuZGVkIjpmYWxzZSwibGFzdF9jaGFuZ2VfcGFzc3dvcmQiOjE3MDU2Mjk0MDcsImxhc3RfY29uZmlybV9vdHAiOjE3MDU2MjkzMzUsImNpZF9oYXNoIjoiMEE2QjcyNzhDNkUxQ0JDQ0E3MzNGNzEzNDRCODhDMTk6NTUiLCJjaWRfZW5jcnlwdCI6IjQ4NjQ4QjU2MkQ2NTY2QUJFOUZBNTIyOUVENjUwNEUxMjY3NDk3REE4OUE1N0FDNjJGODdFMzQyM0YyNTZEQTU2RTg2MUVDNUI2QUFBQjJFOUI1MDkyRURCMiIsImNpZF9hZXMiOiJLVHJOMitLbFYwRC9oaS9jbmcvclJnPT0iLCJjbGllbnRfaXAiOiIxNzEuNC4yMjAuOTIiLCJzY29wZSI6W3siY29kZSI6IklNTVVOSVpBVElPTl9WSUVXOjMifSx7ImNvZGUiOiJJTU1VTklaQVRJT05fVVBEQVRFOjMifSx7ImNvZGUiOiJNT1BIX0FDQ09VTlRfQ0VOVEVSX0FETUlOOjMifSx7ImNvZGUiOiJJTU1VTklaQVRJT05fUEVSU09OX1VQTE9BRDozIn0seyJjb2RlIjoiSU1NVU5JWkFUSU9OX0RBU0hCT0FSRDozIn0seyJjb2RlIjoiSU1NVU5JWkFUSU9OX1NMT1Q6MyJ9LHsiY29kZSI6IklNTVVOSVpBVElPTl9RVU9UQTozIn0seyJjb2RlIjoiSU1NVU5JWkFUSU9OX1JFUE9SVDozIn0seyJjb2RlIjoiSU1NVU5JWkFUSU9OX1JFUE9SVF9FWENFTDozIn0seyJjb2RlIjoiSU1NVU5JWkFUSU9OX0NPTVBBTlk6MyJ9LHsiY29kZSI6IklNTVVOSVpBVElPTl9MQUI6MyJ9LHsiY29kZSI6IklNTVVOSVpBVElPTl9BRUZJX1VQREFURTozIn0seyJjb2RlIjoiSU1NVU5JWkFUSU9OX1NMT1RfTUFOQUdFUjozIn0seyJjb2RlIjoiSU1NVU5JWkFUSU9OX0VQSURFTTozIn0seyJjb2RlIjoiTU9QSF9IT01FX0lTT0xBVElPTjozIn0seyJjb2RlIjoiTU9QSF9IT01FX0lTT0xBVElPTl9BRE1JTjozIn0seyJjb2RlIjoiRVBJREVNX1VQREFURURBVEE6MyJ9LHsiY29kZSI6IkVQSURFTV9SRVBPUlQ6MyJ9LHsiY29kZSI6Ik1PUEhfUEhSX0hJRTozIn0seyJjb2RlIjoiTU9QSF9QSFJfREFTSEJPQVJEOjMifSx7ImNvZGUiOiJNT1BIX1BIUl9EQVNIQk9BUkRfUkVQT1JUOjMifSx7ImNvZGUiOiJNT1BIX0lEUF9BRE1JTjozIn0seyJjb2RlIjoiTU9QSF9JRFBfQVBJOjMifSx7ImNvZGUiOiJNT1BIX0NMQUlNOjMifSx7ImNvZGUiOiJNT1BIX0NMQUlNX0FQSTozIn0seyJjb2RlIjoiTU9QSF9DTEFJTV9BRE1JTjozIn0seyJjb2RlIjoiRVBJREVNX0RPV05MT0FEX0RBVEE6MyJ9LHsiY29kZSI6IklNTVVOSVpBVElPTl9WSUVXOjEifSx7ImNvZGUiOiJJTU1VTklaQVRJT05fVVBEQVRFOjEifSx7ImNvZGUiOiJNT1BIX0FDQ09VTlRfQ0VOVEVSX0FETUlOOjEifSx7ImNvZGUiOiJJTU1VTklaQVRJT05fUEVSU09OX1VQTE9BRDoxIn0seyJjb2RlIjoiSU1NVU5JWkFUSU9OX0RBU0hCT0FSRDoxIn0seyJjb2RlIjoiSU1NVU5JWkFUSU9OX1NMT1Q6MSJ9LHsiY29kZSI6IklNTVVOSVpBVElPTl9RVU9UQToxIn0seyJjb2RlIjoiSU1NVU5JWkFUSU9OX0xBQjoxIn0seyJjb2RlIjoiSU1NVU5JWkFUSU9OX1NMT1RfTUFOQUdFUjoxIn0seyJjb2RlIjoiRVBJREVNX1VQREFURURBVEE6MSJ9LHsiY29kZSI6IkVQSURFTV9SRVBPUlQ6MSJ9LHsiY29kZSI6IklNTVVOSVpBVElPTl9SRVBPUlRfRVhDRUw6MSJ9LHsiY29kZSI6IklNTVVOSVpBVElPTl9FUElERU06MSJ9LHsiY29kZSI6Ik1PUEhfUEhSX0hJRToxIn0seyJjb2RlIjoiTU9QSF9GT1JFSUdOX0lEUDoxIn0seyJjb2RlIjoiTU9QSF9QSFJfREFTSEJPQVJEOjEifSx7ImNvZGUiOiJNT1BIX1BIUl9EQVNIQk9BUkRfUkVQT1JUOjEifSx7ImNvZGUiOiJNT1BIX0lEUF9BRE1JTjoxIn0seyJjb2RlIjoiTU9QSF9JRFBfQVBJOjEifSx7ImNvZGUiOiJNT1BIX0NMQUlNOjEifSx7ImNvZGUiOiJNT1BIX0NMQUlNX0FQSToxIn0seyJjb2RlIjoiTU9QSF9DTEFJTV9BRE1JTjoxIn1dLCJyb2xlIjpbIm1vcGgtYXBpIl0sInNjb3BlX2xpc3QiOiJbSU1NVU5JWkFUSU9OX1ZJRVc6M11bSU1NVU5JWkFUSU9OX1VQREFURTozXVtNT1BIX0FDQ09VTlRfQ0VOVEVSX0FETUlOOjNdW0lNTVVOSVpBVElPTl9QRVJTT05fVVBMT0FEOjNdW0lNTVVOSVpBVElPTl9EQVNIQk9BUkQ6M11bSU1NVU5JWkFUSU9OX1NMT1Q6M11bSU1NVU5JWkFUSU9OX1FVT1RBOjNdW0lNTVVOSVpBVElPTl9SRVBPUlQ6M11bSU1NVU5JWkFUSU9OX1JFUE9SVF9FWENFTDozXVtJTU1VTklaQVRJT05fQ09NUEFOWTozXVtJTU1VTklaQVRJT05fTEFCOjNdW0lNTVVOSVpBVElPTl9BRUZJX1VQREFURTozXVtJTU1VTklaQVRJT05fU0xPVF9NQU5BR0VSOjNdW0lNTVVOSVpBVElPTl9FUElERU06M11bTU9QSF9IT01FX0lTT0xBVElPTjozXVtNT1BIX0hPTUVfSVNPTEFUSU9OX0FETUlOOjNdW0VQSURFTV9VUERBVEVEQVRBOjNdW0VQSURFTV9SRVBPUlQ6M11bTU9QSF9QSFJfSElFOjNdW01PUEhfUEhSX0RBU0hCT0FSRDozXVtNT1BIX1BIUl9EQVNIQk9BUkRfUkVQT1JUOjNdW01PUEhfSURQX0FETUlOOjNdW01PUEhfSURQX0FQSTozXVtNT1BIX0NMQUlNOjNdW01PUEhfQ0xBSU1fQVBJOjNdW01PUEhfQ0xBSU1fQURNSU46M11bRVBJREVNX0RPV05MT0FEX0RBVEE6M11bSU1NVU5JWkFUSU9OX1ZJRVc6MV1bSU1NVU5JWkFUSU9OX1VQREFURToxXVtNT1BIX0FDQ09VTlRfQ0VOVEVSX0FETUlOOjFdW0lNTVVOSVpBVElPTl9QRVJTT05fVVBMT0FEOjFdW0lNTVVOSVpBVElPTl9EQVNIQk9BUkQ6MV1bSU1NVU5JWkFUSU9OX1NMT1Q6MV1bSU1NVU5JWkFUSU9OX1FVT1RBOjFdW0lNTVVOSVpBVElPTl9MQUI6MV1bSU1NVU5JWkFUSU9OX1NMT1RfTUFOQUdFUjoxXVtFUElERU1fVVBEQVRFREFUQToxXVtFUElERU1fUkVQT1JUOjFdW0lNTVVOSVpBVElPTl9SRVBPUlRfRVhDRUw6MV1bSU1NVU5JWkFUSU9OX0VQSURFTToxXVtNT1BIX1BIUl9ISUU6MV1bTU9QSF9GT1JFSUdOX0lEUDoxXVtNT1BIX1BIUl9EQVNIQk9BUkQ6MV1bTU9QSF9QSFJfREFTSEJPQVJEX1JFUE9SVDoxXVtNT1BIX0lEUF9BRE1JTjoxXVtNT1BIX0lEUF9BUEk6MV1bTU9QSF9DTEFJTToxXVtNT1BIX0NMQUlNX0FQSToxXVtNT1BIX0NMQUlNX0FETUlOOjFdIiwiYWNjZXNzX2NvZGVfbGV2ZWwxIjoiJyciLCJhY2Nlc3NfY29kZV9sZXZlbDIiOiInJyIsImFjY2Vzc19jb2RlX2xldmVsMyI6Iic1MDAwMDAnIiwiYWNjZXNzX2NvZGVfbGV2ZWw0IjoiJyciLCJhY2Nlc3NfY29kZV9sZXZlbDUiOiInJyJ9fQ.h3tNgquKx5bL7brYVHw78V5oWSWL07asZwWx6KotuqhfHD5b9Zlb1Gc72emj90ociNrElAq_tbMQeIaQA3MjZmHF6Y_uTrCRBVIhGDELu_T67rTdN1UuaBs9r-8WcB43UoklJ73nisfhfiQDpJWZS9ze43zJqqOXsQGD8ge6D7ecuhtV7Z4hx-io_K1wjqw3RPUAW2EK12BxMXJNZ4tSbMhTFAjYHJcBaBzcX8pYTu1NIIeawFary0R8I1FgqNtUpcAf9j0iBGmIQL-Y2xz9KHZvVZzA4kRaUmm5BzevykuoUQcy9tAkBf-ZXlu_CslYr_46zSKDzVdwp2JZDNcFZXJLd422k5fc7POlU6xb-tc8k0kQf-Vip0pRlGng4EU_A-Q4KMkHwAgi2r7SFZWfkWV6CleqE-qHA5Ze_sN3IVzRQus-os3VnvbTN5pdFnf7GG0rv1CHTKlfz1EmwBWGFz6T3HXnMjf_mJwjUjI3qjSFy3waT4F0rUSkdUHabXFyp4PjEoi0Xy7o51nNA_0XlBVaagyUaRZBa_5-efplq7Ipuye4TQej9WQvow-47dEruOSc9Lm4RZPe4PrtxVMqM3tz5UBZVs4MvtpXwMFVR47J5i31XCLAnyXhFdfjlCq7758hwMNivcGmK_Bn9xKGqpgc-_Luzql1cdeYq5K_1O0"


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
