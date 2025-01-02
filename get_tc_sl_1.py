from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from datetime import datetime, timedelta, timezone
import time
import csv
import boto3
from botocore.errorfactory import ClientError
import json

BUCKET_NAME = 'ypahuara-tc-cs'
OBJECT_PREFIX = 'replication/prestamype/tc_contable'
PATH_SCRAP_WEB = 'https://www.sbs.gob.pe/app/pp/SISTIP_PORTAL/Paginas/Publicacion/TipoCambioContable.aspx'
TAKEN_BACK_DAYS = 10

def write2csv(date_searched, tc, s3_client):
    # with open(path,'a', newline='') as fd:
    #     writer = csv.writer(fd)
    #     writer.writerow([date_searched.strftime('%Y-%m-%d 00:00:00'), tc])
    
    
    # with open(DOWNLOAD_PATH, 'a', newline='') as fd:
    #     writer = csv.writer(fd)
    #     writer.writerow([date_searched.strftime('%Y-%m-%d 00:00:00'), tc])
    # s3_client.upload_file(DOWNLOAD_PATH, BUCKET_NAME, OBJECT_KEY)
    
    
    body = (f'tc_date,tc_contable\n{date_searched.strftime("%Y-%m-%d 00:00:00")},{tc}').encode('utf-8')
    key = f'{OBJECT_PREFIX}/data_tc_{date_searched.strftime("%Y%m%d")}.csv'
    s3_client.put_object(Body=body, Bucket=BUCKET_NAME, Key=key)

    #temp
    # file_path = "./temp.csv"

    # with open(file_path, "wb") as file:
    #     file.write(body)
    # print(f'test: {date_searched}-{tc}')

def get_message(status_code, date_searched, tc=None):
    if status_code == 404:
        message = f'Error at date {date_searched.strftime("%d/%m/%Y")}'
    elif status_code == 400:
        message = f'Date {date_searched.strftime("%d/%m/%Y")} was already requested'
    elif status_code == 200:
        message = f'Data ok at date {date_searched.strftime("%d/%m/%Y")}, tc is "{tc}"'
    return {
        'statusCode': status_code,
        'body': json.dumps({'date_searched': date_searched.strftime('%d/%m/%Y'), 'tc_contable': tc, 'message': message})
    }

def date_already_requested(date_searched, s3_client):
    key = f'{OBJECT_PREFIX}/data_tc_{date_searched.strftime("%Y%m%d")}.csv'
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
        print(f"Key: '{key}' found!")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"Key: '{key}' does not exist!")
        return False

    # with open(DOWNLOAD_PATH) as downloaded_file:
    #     rows = list(csv.reader(downloaded_file))
    #     last_item = rows[-1]
    #     tc_from_file = last_item[1] if last_item[0] == date_searched.strftime('%Y-%m-%d 00:00:00') else None
    #     return tc_from_file

def main():
    session = boto3.Session(profile_name='p2p-role')
    s3_client = session.client('s3')
    # s3_client = ''
    # s3_client.download_file(BUCKET_NAME, OBJECT_KEY, DOWNLOAD_PATH)

    date2search = (datetime.now(timezone.utc) - timedelta(hours=5) - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    # date2search = datetime.strptime('22/08/2024', '%d/%m/%Y')
    date2search_iteration = date2search

    tc_contable = None
    already_requested = date_already_requested(date2search, s3_client)
    if already_requested:
        return get_message(400, date2search)
    
    if date2search_iteration.weekday() == 6:
        write2csv(date2search, tc_contable, s3_client)
        return get_message(200, date2search, tc_contable)
    else:
        if date2search_iteration.weekday() == 5: date2search_iteration = date2search_iteration - timedelta(days=1)
        driver = webdriver.Chrome()
        driver.get(PATH_SCRAP_WEB)
        try_check = 0
        while (tc_contable == None) and ((date2search - date2search_iteration).days <= TAKEN_BACK_DAYS):
            try:
                input_field = driver.find_element(By.NAME, 'ctl00$cphContent$rdpDate$dateInput')
                input_field.clear()
                input_field.send_keys(date2search_iteration.strftime('%d/%m/%Y'))
                input_field.send_keys(Keys.RETURN)

                time.sleep(1.5) # Espera para que la web obtenga la nueva data

                datos = driver.find_elements(By.CSS_SELECTOR, '.rgMultiHeaderRow > .rgHeader.APLI_fila2:last-child')[0]
                tc_contable = float(datos.text) if datos.text != '' else None
                try_check = 0
                date2search_iteration = date2search_iteration - timedelta(days=1)
            except Exception as e:
                driver.get(PATH_SCRAP_WEB)
                if try_check < 1: # Tiene hasta 2 intentos
                    try_check += 1
                else:
                    try_check = 0
                    date2search_iteration = date2search_iteration - timedelta(days=1)

        driver.close()
        if (tc_contable == None):
            return get_message(404, date2search)
        else:
            write2csv(date2search, tc_contable, s3_client)
            return get_message(200, date2search, tc_contable)
        # print(date2search, tc_contable, BUCKET_NAME, OBJECT_KEY)

if __name__ == '__main__':
    start_time = time.time()
    print(f"--- {(time.time() - start_time)} seconds ---")
    result = main()
    print(result)
    print("--- Get data %s seconds ---" % (time.time() - start_time))
