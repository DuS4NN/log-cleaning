import datetime as dt
import os
import itertools
import re
import pandas as pd
import math
import csv

root = 'https://www.ukf.sk'


def find_date():
    # Dátumy zo zadania
    date_start = dt.date(2017, 7, 9)
    date_end = dt.date(2017, 7, 15)

    # Otvorím si celý log a rozložím ho na riadky
    original_file = open('wm2020projekt.log')
    original_log = original_file.read().split('\n')

    if os.path.exists('log_projekt.csv'):
        os.remove('log_projekt.csv')
    file_writer = open('log_projekt.csv', 'a')

    # Prehľadávam celý log po riadkoch
    for row in original_log:
        try:
            # Nájdem dátum daného riadku
            string_date = row.split(' ')[3].replace('[', '')
            date = dt.datetime.strptime(string_date, "%d/%b/%Y:%H:%M:%S")
            # Ak sa dátum riadku nachádza v zadanom intervale zapíšem riadok do nového súboru
            if date_start <= date.date() <= date_end:
                file_writer.write(row + '\n')

        except Exception as e:
            print(str(e))

    file_writer.close()
    original_file.close()


def clear_log():
    banned_request = ['HEAD /', 'POST /', '.json', '.bmp', '.css', '.ico', '.svg', '.flv', '.jpeg', '.swf', '.JPG','.rss', '.xml', '.cur', '.eot', '.js', '.ttf', '.woff', '.png', '.otf', '.gif', '.jpg', 'GET /navbar/navbar-ukf.html HTTP/1.', 'GET /robots.txt']
    banned_ip = ['65.55.110.205', '74.6.18.226', '77.88.27.25']
    banned_response = ['1', '4', '5']
    banned_agent = ['bot', 'crawler', 'crawl', 'spider', 'curl', 'ltx71', 'libwww-perl', 'Microsoft-WebDAV-MiniRedir']

    original_file = open('log_projekt.csv')
    original_log = original_file.read().split('\n')

    clean_log_list = []

    for row in original_log:
        split_log = row.split('\"')
        write = True

        if len(split_log) < 5:
            continue

        if split_log[1].find('GET /robots.txt') is not -1:
            banned_ip.append(split_log[0].split(' ')[0])
            write = False

        for (a, b, c, d) in itertools.zip_longest(banned_request, banned_ip, banned_agent, banned_response):
            if split_log[1].find(str(a)) is not -1 or split_log[0].find(str(b)) is not -1 or split_log[5].find(str(c)) is not -1 or split_log[2][1].find(str(d)) is not -1:
                write = False

        if write:
            time_stamp = re.findall('\d+[\/]\w+[\/]\d+:\d+:\d+:\d+', split_log[0])[0]
            ip_date = split_log[0].split(' ')
            response_size = split_log[2].split(' ')

            clean_row = [ip_date[0], ip_date[3][1:], split_log[1], response_size[1], response_size[2], split_log[3], split_log[5], str(dt.datetime.strptime(time_stamp, "%d/%b/%Y:%H:%M:%S").timestamp())]
            clean_log_list.append(clean_row)

    original_file.close()

    df = pd.DataFrame(clean_log_list, columns=['IP', 'DateTime', 'Request', 'Response', 'Size', 'Referer', 'Agent', 'TimeStamp'])
    if os.path.exists('clean_log.csv'):
        os.remove('clean_log.csv')
    df.to_csv('clean_log.csv', sep=',')


def session_length_log():

    df = pd.read_csv('clean_log.csv', sep=',')
    df = df.sort_values(['IP', 'Agent', 'TimeStamp'])
    df.reset_index(drop=True, inplace=True)

    last_row = df.iloc[0]
    user_id = 0
    last_index = 0

    for index, row in df.iterrows():
        if str(row['IP']) != str(last_row['IP']) or str(row['Agent']) != str(last_row['Agent']):
            user_id += 1
        else:
            session_length = float(row['TimeStamp']) - float(last_row['TimeStamp'])
            if session_length < 3600:
                df.at[last_index, 'Length'] = int(float(row['TimeStamp']) - float(last_row['TimeStamp']))

        df.at[index, 'UserID'] = int(user_id)
        last_row = row
        last_index = index

    session_rlength_log(df)


def session_rlength_log(df):
    c = (-math.log(1 - 0.4)) / (1 / df['Length'].mean())

    last_row = df.iloc[0]
    session_id = 0
    session_length = 0

    for index, row in df.iterrows():
        if row['UserID'] == last_row['UserID']:
            session_length += (float(row['TimeStamp']) - float(last_row['TimeStamp']))
            if session_length > c:
                session_id += 1
                session_length = 0
        else:
            session_id += 1
            session_length += 0

        df.at[index, 'RLengthID'] = int(session_id)
        last_row = row

    if os.path.exists('clean_session_log.csv'):
        os.remove('clean_session_log.csv')
    df.RLengthID = df.RLengthID.astype(int)
    df.UserID = df.UserID.astype(int)
    df.TimeStamp = df.TimeStamp.astype(int)
    df.to_csv('clean_session_log.csv', sep=',')


def find_back(rlength_session, row_final):
    # rlength_session = celá kategória, row_final = aktuálny záznam na ktorom sa hl. algoritmus nachádza (teda riadok na ktorom bolo použité späť)
    new_path = []
    # final_page obsahuje Referer aktuálneho riadku, takže ide o stránku ku ktorej hladáme cestu
    final_page = row_final['Referer']
    # Kedže nepotrebujeme celú session vytvoríme si novú len s riadkami, ktoré potrebujeme prezerať, teda riadky pred final_row
    new_session = rlength_session.loc[rlength_session['ID'] < row_final['ID']]

    # Začneme iterovať kategóriu
    for index, one_row in new_session.iterrows():
        # Vytvorím si nový riadok, v ktorom prehodím referer a request
        new_row = row_final.copy()
        new_row['Request'] = 'GET /' + one_row['Referer'].replace('https://www.ukf.sk/', '') + ' HTTP/1.1'
        new_row['Referer'] = root + one_row['Request'].split()[1]
        new_row['Length'] = 1

        # Ak sa request rovná final_page (teda stránke, ktorú hľadáme) našla sa cestu a vrátim ju
        if root + one_row['Request'].split(' ')[1] == final_page:
            new_path.append(row_final)
            return new_path
        else:
            if len(new_path) == 0 or new_path[-1]['Request'] != new_row['Request']:
                new_path.append(new_row)
    return None


def find_path():
    # Prečítam súbor
    df = pd.read_csv('clean_session_log.csv', sep=',', low_memory=False)
    df = df.reset_index(drop=True)
    df = df.drop(df.columns[[1]], axis=1)
    df.rename(columns={df.columns[0]: "ID"}, inplace=True)
    clean_complete_log = []

    # Vytvorím si file writer, ktorým budem zapisovať do nového (finálneho) CSV súboru
    if os.path.exists('finished_log.csv'):
        os.remove('finished_log.csv')
    file_writer = csv.writer(open('finished_log.csv', 'w'), lineterminator='\n')
    # Veľký log budem prechádzať po menších kategória podľa RLengthID
    for i in range(df['RLengthID'].max()):
        # V DF je uložených iba pár záznamok s rovnakým RLengthID, pre lepšie prechádzanie si DataFrame otočím
        rlength_session = df.loc[df['RLengthID'] == i][::-1]
        copy_rlength_session = df.loc[df['RLengthID'] == i][::-1]
        complete_session = []
        length = -1

        # V session sa nachádza iba jeden záznam - zapíšem
        if len(rlength_session) == 1:
            clean_complete_log.append(rlength_session)

        # Prechádzam jednotlivé riadky v DF
        for index, row in rlength_session.iterrows():
            length += 1

            # Posledný prvok (resp. prvý pretože pole je obrátené) - zapíšem
            if row['ID'] == rlength_session.iloc[len(rlength_session) - 1]['ID']:
                complete_session.append(row)
                continue

            # Ak sa rovná / znamená to, že ide o request na hl. stránku - počítam s tým, že na hl. stránku sa dá dostať z ktorejkoľvek stránky - zapíšem
            if row['Request'].split(' ')[1] == '/':
                complete_session.append(row)
                continue

            # Refreshol stránku - zapíšem
            if root + row['Request'].split(' ')[1] == row['Referer']:
                complete_session.append(row)
                continue

            # Preklikol sa cez inú stránku ako UKF - zapíšem
            if 'ukf.sk' not in str(row['Referer']):
                complete_session.append(row)
                continue

            # Zobrazovanie fotiek na stranke sposobuje problemy v algoritme findBack preto prehladavanie galerie vynecham
            if '/foto/' in row['Request']:
                complete_session.append(row)
                continue

            # Referer sa rovná s predchádzajúcim requestom, takže sa klient pohybuje normálne po stránke - zapíšem
            if row['Referer'] == root + rlength_session.iloc[length + 1]['Request'].split(' ')[1]:
                complete_session.append(row)
                continue

            # Použil tlačidlo späť ? - nájdi
            back = find_back(copy_rlength_session, row)
            if back is not None:
                print(row['ID'])
                for b in back[::-1]:
                    complete_session.append(b)
                continue

            # Nesplňuje žiadne podmienky - zapíšem
            complete_session.append(row)

        # Po prejdení celej kategórie sa otočí do pôvodného stavu a zapíše sa do súboru
        for cs in complete_session[::-1]:
            new_complete_row = [str(cs['IP']), str(cs['DateTime']), str(cs['Request']), str(cs['Response']),
                                str(cs['Size']), str(cs['Referer']), str(cs['Agent']), str(cs['TimeStamp']),
                                str(cs['Length']), str(cs['UserID']), str(cs['RLengthID'])]
            clean_complete_log.append(new_complete_row)
            file_writer.writerow(new_complete_row)


find_date()
clear_log()
session_length_log()
find_path()