import requests
import time
import json

from datetime import timedelta, datetime, date
from threading import Thread

try:
    from save_db_mysql import load_db, get_id_country_db, get_match_id_in_db
    from save_db_mysql import update_line_db, get_tip_in_db, get_vips_match_id_in_db
    from save_db_mysql import load_vips_db, get_id_in_vip_db, get_lines_db, generate_slugs
except Exception:  # pragma: no cover - optional dependencies
    load_db = lambda *a, **k: None
    get_id_country_db = lambda *a, **k: None
    get_match_id_in_db = lambda *a, **k: None
    update_line_db = lambda *a, **k: None
    get_tip_in_db = lambda *a, **k: None
    get_vips_match_id_in_db = lambda *a, **k: None
    load_vips_db = lambda *a, **k: None
    get_id_in_vip_db = lambda *a, **k: None
    get_lines_db = lambda *a, **k: None
    generate_slugs = lambda *a, **k: None

try:
    from good_ligis import sp_good_ligs
except Exception:  # pragma: no cover - optional dependency
    sp_good_ligs = []



SPORTS = [1,3,4,6]
# Football - 1
# Tennis - 2
# Basketball - 3
# Hockey - 4
# American football - 5
# Baseball - 6
# Handball - 7
# Volleyball - 12
# Badminton - 21
# Table tennis - 25




def podkluchenie(url):
    #===================== подключение к сайту ====================
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0',
        'Accept': '*/*',
        #'Host':'global.flashscore.ninja',
        'Origin':'https://www.flashscore.com',
        'Referer':'https://www.flashscore.com/',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',        
        'x-fsign': 'SW9D1eZo',        
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'X-Firefox-Spdy':'h2',
        'x-geoip2-city-name':'London',
        'x-geoip2-country-code':'GB',
        'x-geoip2-iso-subdivision-code-0':'GB-ENG',
        'x-geoip2-subdivision-code-0':'ENG',
        'x-geoip2-subdivision-name-0':'England',
        'x-geoip2-subdivision-name-1':'',
        #'x-vname':'lsproxy28/varnish1[P]|lsbproxy1/varnish1[A]',
        #'x-ttlset':'BEH[P]|BEH[A]'
        } 
    
    r = ''
    i = 0
    while i<3:
        i+=1
        try:            
            r = requests.get(url, headers=header, timeout=7)            
            if r.status_code == 200:
                #print(r.status_code)
                break
            else:                
                time.sleep(0.1)                
                continue
            
        except Exception as e:
            vremya_oshibki = datetime.now().strftime("%d_%m_%Y %H:%M:%S")
            print(f'[{vremya_oshibki}]: Ошибка подключения: {e}\n{url}')            
            time.sleep(0.1)
            continue
    
    return r


def poisk_h2h_json(dannie_text):
    # поиск игр н2н    
    dannie_matchs = {"GROUP_LABEL":"", "ITEMS":[]}
    stage = {'3':'FINISHED','5':'CANCELLED','4':'Postponed','11':'After Penalties','10':'After extra time','54':'Those. defeat','9':'No-show'}
    result = {"l":"LOST","w":"WIN","d":"DRAW", "lo":"LOST","wo":"WIN"}
    
    #{"START_TIME":1723852800,"EVENT_ID":"rRvgtSje","EVENT_NAME":"Liga Pro","STAGE":"FINISHED","COUNTRY_ID":68,"COUNTRY_NAME":"Ecuador",
    # "EVENT_ACRONYM":"LP","HOME_PARTICIPANT":"Libertad","HOME_PARTICIPANT_NAME_ONE":"Libertad","HOME_PARTICIPANT_NAME_TWO":null,
    # "AWAY_PARTICIPANT":"*Macara","AWAY_PARTICIPANT_NAME_ONE":"Macara","AWAY_PARTICIPANT_NAME_TWO":null,"CURRENT_RESULT":"0:1",
    # "HOME_SCORE_FULL":"0","AWAY_SCORE_FULL":"1","HOME_IMAGES":["https://www.flashscore.com/res/image/data/QN6O3vXg-QRv6EbJC.png"],
    # "AWAY_IMAGES":["https://www.flashscore.com/res/image/data/4tKF7Wh5-0rjUYb4J.png"],"H_RESULT":"WIN","TEAM_MARK":"away"}
    
    try:
        h2hs_homes = dannie_text.split('¬~')        
        dannie_matchs["GROUP_LABEL"] = h2hs_homes[0]
        #print(h2hs_homes[0])
        for h2h in h2hs_homes[1:]:
            match_item = {"START_TIME":"","EVENT_ID":"","EVENT_NAME":"","STAGE":"","COUNTRY_ID":"","COUNTRY_NAME":"",
                  "EVENT_ACRONYM":"","HOME_PARTICIPANT":"","HOME_PARTICIPANT_NAME_ONE":"","HOME_PARTICIPANT_NAME_TWO":None,
                  "AWAY_PARTICIPANT":"","AWAY_PARTICIPANT_NAME_ONE":"","AWAY_PARTICIPANT_NAME_TWO":None,"CURRENT_RESULT":"",
                  "HOME_SCORE_FULL":"","AWAY_SCORE_FULL":"","HOME_IMAGES":[""],"AWAY_IMAGES":[""],"H_RESULT":"","TEAM_MARK":""}
            try:               
                match_item["EVENT_ID"] = h2h.split('KP÷')[1].split('¬')[0]
                match_item["START_TIME"] = int(h2h.split('KC÷')[1].split('¬')[0])                
                match_item["EVENT_NAME"] = h2h.split('KF÷')[1].split('¬')[0]
                try:
                    match_item["STAGE"] = stage[h2h.split('AC÷')[1].split('¬')[0]]
                except:
                    #print(h2h.split('KP÷')[1].split('¬')[0], h2h.split('AC÷')[1].split('¬')[0])
                    pass
                    
                match_item["COUNTRY_ID"] = int(h2h.split('KG÷')[1].split('¬')[0])
                match_item["COUNTRY_NAME"] = h2h.split('KH÷')[1].split('¬')[0]
                match_item["EVENT_ACRONYM"] = h2h.split('KI÷')[1].split('¬')[0]                
                match_item["HOME_PARTICIPANT"] = h2h.split('KJ÷')[1].split('¬')[0]
                match_item["HOME_PARTICIPANT_NAME_ONE"] = h2h.split('FH÷')[1].split('¬')[0]
                match_item["AWAY_PARTICIPANT"] = h2h.split('KK÷')[1].split('¬')[0]
                match_item["AWAY_PARTICIPANT_NAME_ONE"] = h2h.split('FK÷')[1].split('¬')[0]
                match_item["CURRENT_RESULT"] = h2h.split('KL÷')[1].split('¬')[0]
                match_item["HOME_SCORE_FULL"] = h2h.split('KU÷')[1].split('¬')[0]
                match_item["AWAY_SCORE_FULL"] = h2h.split('KT÷')[1].split('¬')[0]
                match_item["HOME_IMAGES"][0] = f"https://www.flashscore.com/res/image/data/{h2h.split('EC÷')[1].split('¬')[0]}"
                match_item["AWAY_IMAGES"][0] = f"https://www.flashscore.com/res/image/data/{h2h.split('ED÷')[1].split('¬')[0]}"                                
                match_item["H_RESULT"] = result[h2h.split('KN÷')[1].split('¬')[0]]
                match_item["TEAM_MARK"] = h2h.split('KS÷')[1].split('¬')[0]
                
                dannie_matchs["ITEMS"].append(match_item)
                #print(match_item)
            except:                
                pass
        
    except:
        pass

    return dannie_matchs


def poisk_h2h_json_lichki(dannie_text):
    # поиск игр н2н    
    dannie_matchs = {"GROUP_LABEL":"", "ITEMS":[]}
    stage = {'3':'FINISHED','5':'CANCELLED','4':'Postponed','11':'After Penalties','10':'After extra time','54':'Those. defeat','9':'No-show'}
    result = {"l":"LOST","w":"WIN","d":"DRAW", "lo":"LOST","wo":"WIN"} 
    
    #{"START_TIME":1681587000,"EVENT_ID":"UmCbI8tL","EVENT_NAME":"Liga Profesional","STAGE":"FINISHED","COUNTRY_ID":22,
    # "COUNTRY_NAME":"Argentina","EVENT_ACRONYM":"LPF","HOME_PARTICIPANT":"*Defensa y Justicia","HOME_PARTICIPANT_NAME_ONE":"Defensa y Justicia",
    # #"HOME_PARTICIPANT_NAME_TWO":null,"AWAY_PARTICIPANT":"Instituto","AWAY_PARTICIPANT_NAME_ONE":"Instituto","AWAY_PARTICIPANT_NAME_TWO":null,
    # "CURRENT_RESULT":"1:0","HOME_SCORE_FULL":"1","AWAY_SCORE_FULL":"0",
    # "HOME_IMAGES":["https://www.flashscore.com/res/image/data/WbcBGVf5-QcmznTSR.png"],
    # "AWAY_IMAGES":["https://www.flashscore.com/res/image/data/Y9hE69g5-ImUFDktI.png"]}
    
    try:
        h2hs_homes = dannie_text.split('¬~')        
        dannie_matchs["GROUP_LABEL"] = h2hs_homes[0]
        #print(h2hs_homes[0])
        for h2h in h2hs_homes[1:]:            
            match_item = {"START_TIME":"","EVENT_ID":"","EVENT_NAME":"","STAGE":"","COUNTRY_ID":"","COUNTRY_NAME":"",
                  "EVENT_ACRONYM":"","HOME_PARTICIPANT":"","HOME_PARTICIPANT_NAME_ONE":"","HOME_PARTICIPANT_NAME_TWO":None,
                  "AWAY_PARTICIPANT":"","AWAY_PARTICIPANT_NAME_ONE":"","AWAY_PARTICIPANT_NAME_TWO":None,"CURRENT_RESULT":"",
                  "HOME_SCORE_FULL":"","AWAY_SCORE_FULL":"","HOME_IMAGES":[""],"AWAY_IMAGES":[""]}
            try:               
                match_item["EVENT_ID"] = h2h.split('KP÷')[1].split('¬')[0]
                match_item["START_TIME"] = int(h2h.split('KC÷')[1].split('¬')[0])                
                match_item["EVENT_NAME"] = h2h.split('KF÷')[1].split('¬')[0]
                try:
                    match_item["STAGE"] = stage[h2h.split('AC÷')[1].split('¬')[0]]
                except:
                    #print(h2h.split('KP÷')[1].split('¬')[0], h2h.split('AC÷')[1].split('¬')[0])
                    pass
                    
                match_item["COUNTRY_ID"] = int(h2h.split('KG÷')[1].split('¬')[0])
                match_item["COUNTRY_NAME"] = h2h.split('KH÷')[1].split('¬')[0]
                match_item["EVENT_ACRONYM"] = h2h.split('KI÷')[1].split('¬')[0]                
                match_item["HOME_PARTICIPANT"] = h2h.split('KJ÷')[1].split('¬')[0]
                match_item["HOME_PARTICIPANT_NAME_ONE"] = h2h.split('FH÷')[1].split('¬')[0]
                match_item["AWAY_PARTICIPANT"] = h2h.split('KK÷')[1].split('¬')[0]
                match_item["AWAY_PARTICIPANT_NAME_ONE"] = h2h.split('FK÷')[1].split('¬')[0]
                match_item["CURRENT_RESULT"] = h2h.split('KL÷')[1].split('¬')[0]
                match_item["HOME_SCORE_FULL"] = h2h.split('KU÷')[1].split('¬')[0]
                match_item["AWAY_SCORE_FULL"] = h2h.split('KT÷')[1].split('¬')[0]
                match_item["HOME_IMAGES"][0] = f"https://www.flashscore.com/res/image/data/{h2h.split('EC÷')[1].split('¬')[0]}"
                match_item["AWAY_IMAGES"][0] = f"https://www.flashscore.com/res/image/data/{h2h.split('ED÷')[1].split('¬')[0]}"                                
                                
                dannie_matchs["ITEMS"].append(match_item)
                #print(match_item)
            except:                
                pass
        
    except:
        pass

    return dannie_matchs



def create_h2h_to_json(m_id, short_h, short_a):
    # нахождение результатов н2н    
    h2h_json = {"DATA":[]}    
    
    try:
        url1 = f'https://global.flashscore.ninja/2/x/feed/df_hh_1_{m_id}'       
        #url1 = f'https://local-uk.flashscore.ninja/5/x/feed/df_hh_1_{m_id}'
        r1 = podkluchenie(url1)
        if r1 != '':
            try:                
                all_dannie_text = r1.text
                #print(all_dannie_text)
                t_names = all_dannie_text.split('~KA÷')                
                dannie_text_all = all_dannie_text.split('~KB÷')
                dannie_overall_home = poisk_h2h_json(dannie_text_all[1]) # игры overall
                dannie_overall_away = poisk_h2h_json(dannie_text_all[2]) # игры overall
                dannie_overall_lichki = poisk_h2h_json_lichki(dannie_text_all[3]) # игры overall
                overall_json = {"TAB_NAME":t_names[1].split('¬')[0], "GROUPS":[dannie_overall_home, dannie_overall_away,dannie_overall_lichki]}
                h2h_json["DATA"].append(overall_json)                
                
                dannie_home_home = poisk_h2h_json(dannie_text_all[4]) # игры Дома
                dannie_home_lichki = poisk_h2h_json_lichki(dannie_text_all[5]) # игры Дома
                home_home_json = {"TAB_NAME":f"{short_h} - Home", "GROUPS":[dannie_home_home, dannie_home_lichki]}
                h2h_json["DATA"].append(home_home_json)
                dannie_away_away = poisk_h2h_json(dannie_text_all[6]) # игры Выезд
                away_away_json = {"TAB_NAME":f"{short_a} - Away", "GROUPS":[dannie_away_away, dannie_home_lichki]}
                h2h_json["DATA"].append(away_away_json)                               
            except:
                pass        
        
    except:
        pass
    
    #print(m_id, short_h, short_a)
    #print(json.dumps(h2h_json, ensure_ascii=False))
    return json.dumps(h2h_json, ensure_ascii=False)


# Запись Н2Н в таблицу event_last_matches
def addLastMatchesDB(event_id, matches, lang):
    start_time = int(time.time())
    matches = matches.replace("'", "''")
    
    query = f"INSERT INTO event_last_matches (`event_id`, `create_time`, `matches`, `lang`) VALUES ('{event_id}', FROM_UNIXTIME('{start_time}'),'{matches}', '{lang}');"
    update_line_db('event_last_matches', query)


def add_h2h(event_id, short_h, short_a):
    addLastMatchesDB(event_id, create_h2h_to_json(event_id, short_h, short_a), 'en_IN')
    

def update_h2h(event_id, short_h, short_a):
    start_time = int(time.time())
    matches = create_h2h_to_json(event_id, short_h, short_a).replace("'", "''")
    
    query = f"""UPDATE event_last_matches SET matches='{matches}' WHERE event_id='{event_id}';"""    
    update_line_db('event_last_matches', query)

'''
create_h2h_to_json('WCAkeV1N', 'INS', 'DEF')
input()
'''



def mnogopotok(urls):    
    #============ многопоток ===========
    i1 = 0
    x1 = 0    
    while i1 < len(urls):
        thread_list = []
        for i2 in range(20):            
            try:
                time.sleep(0.1)
                t = Thread(target = parse_match,  args = (urls[i1],))
                thread_list.append(t)
                t.start()
                i1 +=1
                x1 +=1
                if x1%10==0:
                    print(f"remains to process [{x1}] from [{len(urls)}]")                

            except:               
                break
                            
        for t in thread_list:            
            t.join()

##############################################################################
########################## Поиск матчей на странице сайта ####################

def poisk_matchey(i, sport_id):
    """ Поиск матчей на странице сайта """
    
    all_spisok_matchey = []

    url = f'https://local-global.flashscore.ninja/2/x/feed/f_{sport_id}_{i}_3_en_1'
        
    try:
        r = podkluchenie(url)
        r.encoding = 'utf-8'
        text_stranici = r.text.split('~ZA÷')        
        for text_str in text_stranici[1:]:            
            country_id = text_str.split('ZB÷')[1].split('¬')[0]
            liga = text_str.split('¬')[0]
            games = text_str.split('~AA÷')
            for game in games[1:]:
                #print(game)
                try:
                    status = game.split('AC÷')[1].split('¬')[0]
                    id_game = game.split('¬')[0]                    
                    data_matcha = datetime.fromtimestamp(int(game.split('AD÷')[1].split('¬')[0])).strftime('%d.%m.%Y %H:%M')                    
                    team1 = game.split('AE÷')[1].split('¬')[0]
                    team2 = game.split('AF÷')[1].split('¬')[0]
                    
                    short_team1 = game.split('WM÷')[1].split('¬')[0]
                    short_team2 = game.split('WN÷')[1].split('¬')[0]                    
                                           
                    try:
                        sc1 = game.split('AG÷')[1].split('¬')[0]                        
                    except:
                        sc1 = ''
                        
                    try:
                        sc2 = game.split('AH÷')[1].split('¬')[0]                        
                    except:
                        sc2 = ''
                    
                    url_game = f'https://www.flashscore.com/match/{id_game}/#/match-summary'                    

                    #spisok_matchey = [data_matcha, [liga,int(country_id)], team1, team2, '','','',sc1,sc2,'',status, url_game,'',
                    #                  short_team1, short_team2, '', '', sport_id, id_game]
                    
                    spisok_matchey = [data_matcha, [liga,int(country_id)], team1, team2, '','','',sc1,sc2,'',status, url_game,'',
                                      short_team1, short_team2, '', '', '','','','', sport_id, id_game]
                    #print(spisok_matchey)
                    if spisok_matchey not in all_spisok_matchey:
                        all_spisok_matchey.append(spisok_matchey)
                        
                    
                except:
                    pass
    except:
        pass    
        
    return all_spisok_matchey



##############################################################################
##############################################################################



def poisk_h2h(dannie_text, data_matcha):
    # поиск игр н2н    
    dannie_matchs = []
    
    try:
        h2hs_homes = dannie_text.split('¬~')        
        nom = 0
        for h2h in h2hs_homes[1:]:           
            try:               
                id_game = h2h.split('KP÷')[1].split('¬')[0]
                vremya1 = h2h.split('KC÷')[1].split('¬')[0]
                data = datetime.fromtimestamp(int(vremya1)).strftime('%d.%m.%Y')
                #print(data)
                g = int(data_matcha.split('.')[-1])
                m = int(data_matcha.split('.')[1])
                d = int(data_matcha.split('.')[0])
                g1 = int(data.split('.')[-1])
                m1 = int(data.split('.')[1])
                d1 = int(data.split('.')[0])

                data_matcha1 = date(g, m, d)
                
                data1 = date(g1, m1, d1)
                #print(data1, data_matcha1)
                if data1 >= data_matcha1:
                    continue                

                team_home = h2h.split('KJ÷')[1].split('¬')[0].replace('*','').strip().split('(')[0].strip()
                team_away = h2h.split('KK÷')[1].split('¬')[0].replace('*','').strip().split('(')[0].strip()
                
                #print(team_home, team_away)
                try:
                    score = h2h.split('KL÷')[1].split('¬')[0].strip()
                    #score = h2h.split('KM÷')[1].split('¬')[0].split('(')[0].strip()                    
                except:                    
                    score = h2h.split('KM÷')[1].split('¬')[0].split('(')[1].split(')')[0].strip()
                    #print(score)
                
                itog = h2h.split('KN÷')[1].split('¬')[0].strip()

                dannie_matchs.append([team_home, team_away, score, itog])
                #print([team_home, team_away, score, itog])
               
                break
            except:                
                pass
        
    except:
        pass
    
    return dannie_matchs



def parser_h2h(m_id, data_matcha):
    # нахождение результатов н2н
    dannie_winlose = ['','']
    
    try:
        url1 = f'https://global.flashscore.ninja/2/x/feed/df_hh_1_{m_id}'       
        #url1 = f'https://local-uk.flashscore.ninja/5/x/feed/df_hh_1_{m_id}'
        r1 = podkluchenie(url1)
        if r1 != '':
            try:                
                all_dannie_text = r1.text                
                dannie_text_all = all_dannie_text.split('~KB÷')
                
                dannie_text_home = dannie_text_all[4] # игры первой команды
                dannie_text_away = dannie_text_all[6] # игры второй команды                
                
                dannie_home = poisk_h2h(dannie_text_home, data_matcha)
                dannie_away = poisk_h2h(dannie_text_away, data_matcha)
                
                if len(dannie_home) != 0:
                    dannie_winlose[0] = dannie_home[0][-1]
                if len(dannie_away) != 0:
                    dannie_winlose[1] = dannie_away[0][-1]                
            except:
                pass        
        
    except:
        pass    
    
    return dannie_winlose



def poisk_kefa_1x2(line):
    # поиск кэфа 1х2
    
    kefs1 = ['','','','','','']
    
    try:
        books_lines = line.split('~OE÷')    
        for books_line in books_lines[1:2]:
            #print(books_line)
            #book1 = books_line.split('OD÷')[1].split('¬')[0]        
            kef_1x2_1 = books_line.split('¬XA÷')[1].split('¬')[0]
            try:
                pon_1 = kef_1x2_1.split('[')[1].split(']')[0]
            except:
                pon_1 = ''
            try:
                kef_1x2_1_nach = kef_1x2_1.split('[')[0].strip()
                kef_1x2_1_konech = kef_1x2_1.split(']')[1].replace(':', '').strip()
            except:
                kef_1x2_1_nach = kef_1x2_1.split('[')[0].strip()
                kef_1x2_1_konech = kef_1x2_1_nach            
            try:
                razn_1 = float(kef_1x2_1_nach)-float(kef_1x2_1_konech)
            except:
                razn_1 = 0
                
            kef_1x2_x = books_line.split('¬XB÷')[1].split('¬')[0]
            try:
                pon_x = kef_1x2_x.split('[')[1].split(']')[0]
            except:
                pon_x = ''
            try:
                kef_1x2_x_nach = kef_1x2_x.split('[')[0].strip()
                kef_1x2_x_konech = kef_1x2_x.split(']')[1].replace(':', '').strip()
            except:
                kef_1x2_x_nach = kef_1x2_x.split('[')[0].strip()
                kef_1x2_x_konech = kef_1x2_x_nach            
            try:
                razn_x = float(kef_1x2_x_nach)-float(kef_1x2_x_konech)
            except:
                razn_x = 0

            kef_1x2_2 = books_line.split('¬XC÷')[1].split('¬')[0]
            try:
                pon_2 = kef_1x2_2.split('[')[1].split(']')[0]
            except:
                pon_2 = ''
                
            try:
                kef_1x2_2_nach = kef_1x2_2.split('[')[0].strip()
                kef_1x2_2_konech = kef_1x2_2.split(']')[1].replace(':', '').strip()
            except:
                kef_1x2_2_nach = kef_1x2_2.split('[')[0].strip()
                kef_1x2_2_konech = kef_1x2_2_nach
            
            try:
                razn_2 = float(kef_1x2_2_nach)-float(kef_1x2_2_konech)
            except:
                razn_2 = 0
   
            kefs1[0] = float(kef_1x2_1_konech)
            kefs1[1] = float(kef_1x2_x_konech)
            kefs1[2] = float(kef_1x2_2_konech)
            kefs1[3] = [pon_1,razn_1]
            kefs1[4] = [pon_x,razn_x]
            kefs1[5] = [pon_2,razn_2]
    except:
        pass
    
    return kefs1

def poisk_kefa_1_2(line):
    # поиск кэфа 1х2
    
    kefs1 = ['','','','','','']
    
    try:
        books_lines = line.split('~OE÷')    
        for books_line in books_lines[1:2]:
            #print(books_line)
            #book1 = books_line.split('OD÷')[1].split('¬')[0]        
            kef_1x2_1 = books_line.split('¬XB÷')[1].split('¬')[0]
            try:
                pon_1 = kef_1x2_1.split('[')[1].split(']')[0]
            except:
                pon_1 = ''
            try:
                kef_1x2_1_nach = kef_1x2_1.split('[')[0].strip()
                kef_1x2_1_konech = kef_1x2_1.split(']')[1].replace(':', '').strip()
            except:
                kef_1x2_1_nach = kef_1x2_1.split('[')[0].strip()
                kef_1x2_1_konech = kef_1x2_1_nach            
            try:
                razn_1 = float(kef_1x2_1_nach)-float(kef_1x2_1_konech)
            except:
                razn_1 = 0                
            

            kef_1x2_2 = books_line.split('¬XC÷')[1].split('¬')[0]
            try:
                pon_2 = kef_1x2_2.split('[')[1].split(']')[0]
            except:
                pon_2 = ''
                
            try:
                kef_1x2_2_nach = kef_1x2_2.split('[')[0].strip()
                kef_1x2_2_konech = kef_1x2_2.split(']')[1].replace(':', '').strip()
            except:
                kef_1x2_2_nach = kef_1x2_2.split('[')[0].strip()
                kef_1x2_2_konech = kef_1x2_2_nach
            
            try:
                razn_2 = float(kef_1x2_2_nach)-float(kef_1x2_2_konech)
            except:
                razn_2 = 0
   
            kefs1[0] = float(kef_1x2_1_konech)
            kefs1[1] = ''
            kefs1[2] = float(kef_1x2_2_konech)
            kefs1[3] = [pon_1,razn_1]
            kefs1[4] = ''
            kefs1[5] = [pon_2,razn_2]
    except:
        pass
    
    return kefs1


def poisk_kefa_OU(books_lines, total):
    # Поиск кэфа Тотал
    kefs1 = ['',total,'','','','']

    try:
        books_line_oc = books_lines.split('~OE÷')
        for books_line in books_line_oc[1:2]:            
            kef_1x2_1 = books_line.split('¬XB÷')[1].split('¬')[0]
            try:
                pon_1 = kef_1x2_1.split('[')[1].split(']')[0]
            except:
                pon_1 = ''
            try:
                kef_1x2_1_nach = kef_1x2_1.split('[')[0].strip()
                kef_1x2_1_konech = kef_1x2_1.split(']')[1].replace(':', '').strip()
            except:
                kef_1x2_1_nach = kef_1x2_1.split('[')[0].strip()
                kef_1x2_1_konech = kef_1x2_1_nach            
            try:
                razn_1 = float(kef_1x2_1_nach)-float(kef_1x2_1_konech)
            except:
                razn_1 = 0                
            

            kef_1x2_2 = books_line.split('¬XC÷')[1].split('¬')[0]
            try:
                pon_2 = kef_1x2_2.split('[')[1].split(']')[0]
            except:
                pon_2 = ''
                
            try:
                kef_1x2_2_nach = kef_1x2_2.split('[')[0].strip()
                kef_1x2_2_konech = kef_1x2_2.split(']')[1].replace(':', '').strip()
            except:
                kef_1x2_2_nach = kef_1x2_2.split('[')[0].strip()
                kef_1x2_2_konech = kef_1x2_2_nach
            
            try:
                razn_2 = float(kef_1x2_2_nach)-float(kef_1x2_2_konech)
            except:
                razn_2 = 0
   
            kefs1[0] = float(kef_1x2_1_konech)
            kefs1[1] = total
            kefs1[2] = float(kef_1x2_2_konech)
            kefs1[3] = [pon_1,razn_1]
            kefs1[4] = ''
            kefs1[5] = [pon_2,razn_2]

    except:
        pass    
    #print(sp_markets)
    return kefs1


def poisk_kef(id_m, sport_id):
    # поиск кэфов 1х2
    
    kefs = ['','','','','','']
    kefs_dc = ['','','','','','']
    kefs_OU = ['','','','','','']
    kefs_bts = ['','','','','','']
    
    try:
        #url_kefs = f'https://global.flashscore.ninja/2/x/feed/df_od_1_{id_m}_'
        url_kefs = f'https://global.flashscore.ninja/2/x/feed/df_od_1_{id_m}_'
        
        r = podkluchenie(url_kefs)
        soup = r.text.split('~OA÷')
        for soup_kef in soup[1:]:                            
            #print(soup_kef.split('¬')[0])
            if sport_id in [1,3,4,5,7]:
                if soup_kef.split('¬')[0] == '1X2':
                    kefs_1x2s = soup_kef.split('~OB÷')
                    for kefs_1x2 in kefs_1x2s[1:]:
                        #print(kefs_1x2.split('¬')[0])
                        if kefs_1x2.split('¬')[0] == 'Full Time':
                            try:
                                kefs = poisk_kefa_1x2(kefs_1x2)                            
                            except:
                                break
                            
                        if kefs_1x2.split('¬')[0] == 'FT including OT':
                            try:
                                kefs = poisk_kefa_1x2(kefs_1x2)                            
                            except:
                                break
            
            if sport_id in [2,6,12,21,25]:
                if soup_kef.split('¬')[0] == 'Home/Away':
                    kefs_1x2s = soup_kef.split('~OB÷')                    
                    for kefs_1x2 in kefs_1x2s[1:]:                        
                        #if sport_id in [25]
                        if kefs_1x2.split('¬')[0] == 'Full Time':
                            try:
                                kefs = poisk_kefa_1_2(kefs_1x2)                            
                            except:
                                break
                        
                        if kefs_1x2.split('¬')[0] == 'FT including OT':
                            try:
                                kefs = poisk_kefa_1_2(kefs_1x2)                            
                            except:
                                break
            
            if soup_kef.split('¬')[0] == 'DC':
                kefs_1x2s = soup_kef.split('~OB÷')
                for kefs_1x2 in kefs_1x2s[1:]:
                    #print(kefs_1x2.split('¬')[0])
                    if kefs_1x2.split('¬')[0] == 'Full Time':
                        try:
                            kefs_dc = poisk_kefa_1x2(kefs_1x2)                            
                        except:
                            break
            
            if soup_kef.split('¬')[0] == 'O/U':
                kefs_1x2s = soup_kef.split('~OB÷')
                for kefs_1x2 in kefs_1x2s[1:]:                
                    if kefs_1x2.split('¬')[0] == 'Full Time':
                        books_liness = kefs_1x2.split('¬OC÷')
                        
                        if sport_id == 1:
                            for books_lines in books_liness[1:]:                                
                                total = books_lines.split('¬')[0]                                
                                if total == '2.5':
                                    try:
                                        kefs_OU = poisk_kefa_OU(books_lines, total)
                                    except:
                                        break
                            
                        elif sport_id == 4:
                            for books_lines in books_liness[1:]:                                
                                total = books_lines.split('¬')[0] 
                                if total == '5.5':
                                    try:
                                        kefs_OU = poisk_kefa_OU(books_lines, total)
                                    except:
                                        break
                    
                    
                    if kefs_1x2.split('¬')[0] == 'FT including OT':
                        if sport_id in [3,5,6,7]:                        
                            sp_kef_OU = []
                            for books_lines in books_liness[1:]:                                
                                total = books_lines.split('¬')[0]                                
                                try:
                                    kef_OU = poisk_kefa_OU(books_lines, total)
                                    sp_kef_OU.append(kef_OU)
                                except:
                                    pass
                                
                            # находим средний тотал
                            if len(sp_kef_OU) != 0:
                                sp_razn = []
                                for t in sp_kef_OU:
                                    try:
                                        razn = t[0] - t[2]
                                        sp_razn.append([abs(razn), t])
                                    except:
                                        pass
                                sp_razn.sort()
                                #print(sp_razn)
                                kefs_OU = sp_razn[0][1]
                
            if soup_kef.split('¬')[0] == 'BTS':                
                kefs_1x2s = soup_kef.split('~OB÷')                    
                for kefs_1x2 in kefs_1x2s[1:]:                        
                    #if sport_id in [25]
                    if kefs_1x2.split('¬')[0] == 'Full Time':
                        try:
                            kefs_bts = poisk_kefa_1_2(kefs_1x2)                            
                        except:
                            break
                    
                    if kefs_1x2.split('¬')[0] == 'FT including OT':
                        try:
                            kefs_bts = poisk_kefa_1_2(kefs_1x2)                            
                        except:
                            break 

    except:
        pass    
    
    #print(kefs)
    #print(kefs_dc)
    return kefs, kefs_dc, kefs_OU, kefs_bts



def parse_img_teams(id_m):    
    home_img = ''
    away_img = ''    
    
    url = f'https://www.flashscorekz.com/match/{id_m}/'
    
    r1 = podkluchenie(url)    
    try:
        dannie_text = r1.text.split('window.environment =')[1].split('"participantsData":')[1]
        #print(dannie_text.split('"home":')[1].split('],')[0])
        home_img = dannie_text.split('"home":')[1].split('],')[0].split('"image_path":"')[1].split('",')[0].split('(')[0].replace('\\/','/').strip()
        away_img = dannie_text.split('"away":')[1].split('],')[0].split('"image_path":"')[1].split('",')[0].split('(')[0].replace('\\/','/').strip()        
    except:        
        pass

    return home_img, away_img


   
def parse_match(match):
    # Сбор информации о матче
    try:
        m_id = match[-1]
        sport_id = match[-2]
        data_matcha = match[0].split(' ')[0]
        wl = parser_h2h(m_id, data_matcha)
        imgh, imga = parse_img_teams(m_id)
        match[15] = imgh
        match[16] = imga
        if wl == ['w', 'l'] or wl == ['l', 'w']:
            match[4] = wl
            kefs_m, kefsdc_m, kefs_OU_m, kefs_bts_m = poisk_kef(m_id, sport_id)
            
            if kefs_m[0] != '' and kefs_m[2] != '':            
                if wl == ['w', 'l']:
                    # Если кэф превышает 2, то берем kefsdc_m
                    if kefs_m[0] < 2 and kefs_m[0] >= 1.2:
                        match[5] = kefs_m[0]
                        match[6] = 'W1'
                        match[9] = kefs_m[3]
                        match[12] = kefs_m[2]
                    else:
                        if kefsdc_m[0] != '':
                            if kefsdc_m[0] >= 1.2 and kefsdc_m[0] <= 1.79:
                                match[5] = kefsdc_m[0]
                                match[6] = 'W1X'
                                match[9] = kefsdc_m[3]
                                match[12] = kefsdc_m[2]
                        
                if wl == ['l', 'w']:
                    if kefs_m[2] < 2 and kefs_m[2] >= 1.2:
                        match[5] = kefs_m[2]
                        match[6] = 'W2'
                        match[9] = kefs_m[5]
                        match[12] = kefs_m[0]
                    else:
                        if kefsdc_m[2] != '':
                            if kefsdc_m[2] >= 1.2 and kefsdc_m[2] <= 1.79:
                                match[5] = kefsdc_m[2]
                                match[6] = 'WX2'
                                match[9] = kefsdc_m[5]
                                match[12] = kefsdc_m[0]
                                
            # Формируем тотал
            # [1.98, '', 1.88, ['d', 0.31999999999999984], '', ['u', -0.2599999999999998]]
            try:
                if kefs_OU_m[0] != '':
                    # стрелка красная вниз и коэф ниже чем противоположный,
                    # а так же вторая стрелка вверх зеленая. 
                    if kefs_OU_m[0] < kefs_OU_m[2]:
                        if kefs_OU_m[3][0] == 'd' and kefs_OU_m[5][0] == 'u':
                            if kefs_OU_m[0] <= 1.95:
                                match[17] = f'O{kefs_OU_m[1]}'
                                match[18] = kefs_OU_m[0]
                    
                    elif kefs_OU_m[0] > kefs_OU_m[2]:
                        if kefs_OU_m[5][0] == 'd' and kefs_OU_m[3][0] == 'u':
                            if kefs_OU_m[2] <= 1.95:
                                match[17] = f'U{kefs_OU_m[1]}'
                                match[18] = kefs_OU_m[2]
            except:
                pass
            
            
            # Формируем BTS
            # [1.73, '', 2.0, ['d', 0.27], '', ['u', -0.27]]
            # ТОЛЬКО ДЛЯ ФУТБОЛА
            if sport_id == 1:            
                try:
                    if kefs_bts_m[0] != '':
                        # стрелка красная вниз и коэф ниже чем противоположный,
                        # а так же вторая стрелка вверх зеленая. 
                        if kefs_bts_m[0] < kefs_bts_m[2]:
                            if kefs_bts_m[3][0] == 'd' and kefs_bts_m[5][0] == 'u':
                                if kefs_bts_m[0] <= 1.95:
                                    match[19] = 'yes'
                                    match[20] = kefs_bts_m[0]
                        
                        elif kefs_bts_m[0] > kefs_bts_m[2]:
                            if kefs_bts_m[5][0] == 'd' and kefs_bts_m[3][0] == 'u':
                                if kefs_bts_m[2] <= 1.95:
                                    match[19] = 'no'
                                    match[20] = kefs_bts_m[2]
                except:
                    pass
    except:        
        pass
    
    return match




def clear_old():
    last_days = datetime.now() - timedelta(days=1117) # create_time
    del_days = datetime.now() - timedelta(days=1365) # start_time

    text_execute = f'DELETE FROM vips WHERE event_id_1 NOT IN (SELECT id FROM event) OR event_id_2 NOT IN (SELECT id FROM event)'
    update_line_db('vips', text_execute)
    
    text_execute = f'DELETE FROM event_last_matches WHERE create_time < "{last_days}" OR event_id NOT IN (SELECT event_id FROM event)'
    update_line_db('event_last_matches', text_execute)
    
    text_execute = f'DELETE FROM event WHERE start_time < "{last_days}" AND (tip = "BROKEN" OR tip = "SCHEDULED" OR tip_result IS NULL)'
    update_line_db('event', text_execute)
    
    text_execute = f'DELETE FROM event WHERE start_time < "{del_days}" AND id NOT IN (SELECT event_id_1 FROM vips) AND id NOT IN (SELECT event_id_2 FROM vips)'
    update_line_db('event', text_execute)
    
    text_execute = 'OPTIMIZE TABLE event;'
    update_line_db('event', text_execute) 
    
    text_execute = 'OPTIMIZE TABLE event_last_matches;'
    update_line_db('event_last_matches', text_execute)  
    

'''
m = ['23.08.2024 20:00', 'SPAIN: LaLiga', 'Celta Vigo', 'Valencia', ['w', 'l'], 1.29, 'W1X', '3', '1', ['d', 0.010000000000000009], '3', 'https://www.flashscore.com/match/8KIT2SzJ/#/match-summary', 1.67, 'CEL', 'VAL', 'https://static.flashscore.com/res/image/data/O2htOWDa-SOY3p1Si.png', 'https://static.flashscore.com/res/image/data/Ioshoye5-MHrRLNUo.png', '8KIT2SzJ']
if m[4] == ['w', 'l'] or m[4] == ['l', 'w']:
    if m[6] != '' and m[5] != '':
        country_id = get_id_country_db(m[1])
        print(country_id)
        if country_id != '':
            mm = [country_id, m[-1], datetime.strptime(m[0], '%d.%m.%Y %H:%M').strftime('%Y.%m.%d %H:%M'),
                  m[2], m[3], m[6], float(m[5]), m[12], m[13], m[14], m[15], m[16]]
            print(mm)
            load_db(mm)
            add_h2h(m[-1])

input()
'''



            

def load_vop_matches(all_spisok_matchey, sport_id):
    # запись новых матчей vips
    # =========================================================
    sl_vip_matches = {}
    # Получение всех ключей в базе за выбранные дни
    day1 = datetime.now()-timedelta(days=2)
    day2 = datetime.now()+timedelta(days=4)        
    sp_vips_id = get_match_id_in_db('event', day1, day2)
    
    day1 = datetime.now()-timedelta(days=1)
    day2 = datetime.now()+timedelta(days=1)
    sp_vips_id_keys = get_id_in_vip_db(day1, day2) # все id матчей в таблице vip
    #print(sp_vips_id_keys)
    if len(sp_vips_id_keys) == 0:
        for m in all_spisok_matchey:
            if m[10] == '1':
                id_m1 = get_vips_match_id_in_db(m[-1]) # находим id записи матча в таблице event
                #print(id_m1, sp_vips_id_keys)
                if id_m1 not in sp_vips_id_keys:
                    if m[9] !='':                        
                        if m[9][0] == 'd':
                            if m[-1] in sp_vips_id:
                                d_m = m[0].split(' ')[0]
                                vip_m = [m[9][1], m[0].split(' ')[0], id_m1, m[2], m[3], m[6], m[5]]
                                try:
                                    sl_vip_matches[d_m].append(vip_m)
                                except:
                                    sl_vip_matches[d_m] = []
                                    sl_vip_matches[d_m].append(vip_m)

        if len(sl_vip_matches) != 0:            
            sp_vip_matches_line = []
            #print(sl_vip_matches)
            for s in sl_vip_matches:
                sp_vip_matches = sl_vip_matches[s]
                sp_vip_matches.sort(reverse=True)
            
                i = 0
                while i < len(sp_vip_matches):
                    try:
                        m1 = sp_vip_matches[i]
                        m2 = sp_vip_matches[i+1]                        
                        
                        if m1[2] != '' and m2[2] != '':
                            rez = round(m1[6]*m2[6], 2)
                            vip_m = [datetime.strptime(m1[1], '%d.%m.%Y').strftime('%Y.%m.%d'), sport_id, m2[2], m1[2],
                                     m1[3],m1[4],m2[3],m2[4],m1[5],m2[5],rez]
                            sp_vip_matches_line.append(vip_m)
                    except:
                        pass
                    i+=2
            
            if len(sp_vip_matches_line) != 0:
                # Пишем vip результаты в таблицу
                for m in sp_vip_matches_line:
                    #print(m)
                    load_vips_db(m)
                    break
    # =========================================================
    # =========================================================




# Обновление результата завершенных матчей таблицы vips
def update_vip_matches():
    # =========================================================
    # Получаем результаты записаных игр
    sp_tip_results = {}
    day1 = datetime.now()-timedelta(days=4)
    day2 = datetime.now()+timedelta(days=4)       
    text_execute =  f"""SELECT id, tip_result FROM event WHERE start_time BETWEEN '{day1}' AND '{day2}'"""
    sp_events = get_lines_db(text_execute)
    #print(sp_events)
    for s in sp_events:
        #print(s)
        if s['tip_result'] != None:
            sp_tip_results[s['id']] = s['tip_result']
            #print(s)
    
    
    text_execute =  f"""SELECT id, tip_result, event_id_2, event_id_1 FROM vips WHERE create_time BETWEEN '{day1}' AND '{day2}'"""
    sp_events = get_lines_db(text_execute)    
    for s in sp_events:
        if s['tip_result'] == None:
            # Обновляем данные результата
            try:                
                # Если id матчей найдены и результат won
                if sp_tip_results[s['event_id_2']] == sp_tip_results[s['event_id_1']] and sp_tip_results[s['event_id_2']] == 'won':                    
                    tip_result = 1
                else:                    
                    tip_result = 0
                
                
                dan = [s['id'], tip_result]
                #print(dan)
                text_execute = f"""UPDATE vips SET tip_result="{dan[1]}" WHERE id={dan[0]};"""                
                update_line_db('vips', text_execute)                
            except:
                pass
    
    # =========================================================
    # =========================================================



def parse_sport(sport_id):
    sl_stage = {'3':'FINISHED','5':'CANCELLED','4':'Postponed',
                '11':'After Penalties','10':'After extra time',
                '54':'Those. defeat','9':'No-show'}
    # Запуск программы """
    # '3' - FINISH
    # '5' - CANCELLED
    # '42' - Awaiting updates
    # '12' - 1st Half
    # '13' - 2nd Half
    # '4' - Postponed
    # '38' - Half Time

    sp_close_match_status = ['3','4','5','11','10','54','9'] # список статусов завершения матча
    
    sp_days = ['-1','0','1','2']
    
    d1 = 1
    for i in sp_days:
        print(f'Processing {d1} of {len(sp_days)} days')
        d1+=1        
        
        sp_ids = []
        all_spisok_matchey = [] # Весь список матчей
        all_spisok_matchey_day = [] # список неначавшихся матчей за день
                        
        # Поиск матчей
        all_spisok_matchey = poisk_matchey(i, sport_id)
                
        all_spisok_matchey_copy = all_spisok_matchey.copy()        
        for m in all_spisok_matchey_copy:            
            country_id = get_id_country_db(m[1][0], sport_id)            
            if country_id != '':                
                # Обновляем h2h
                #update_h2h(m[-1], m[13], m[14])
                if country_id not in sp_good_ligs:                    
                    all_spisok_matchey.remove(m)
            else:                
                # Добавляем отсутствующую лигу в список
                text_execute = f"""INSERT INTO cup
                                    (name,country_name,country_id,active,sport_id,on_top)
                                    VALUES ("{m[1][0]}","{m[1][0].split(':')[0]}",{m[1][1]},1,{sport_id},"0")"""
                update_line_db('event', text_execute)
                # Но матч не парсим
                all_spisok_matchey.remove(m)
                
        
        print(f"Found {len(all_spisok_matchey)} matches")
        print(f"Starting to select matches...")
        
        if len(all_spisok_matchey) != 0:
            mnogopotok(all_spisok_matchey)       
                       
            print(f"Making a record...")            
            # Получение всех ключей в базе за выбранный период времени
            day1 = datetime.now()-timedelta(days=3)
            day2 = datetime.now()+timedelta(days=7)        
            sp_ids = get_match_id_in_db('event', day1, day2)
            
            # запись новых матчей в event
            for m in all_spisok_matchey:
                #print(m)
                if m[10] == '1':
                    if m[-1] not in sp_ids:
                        if m[4] == ['w', 'l'] or m[4] == ['l', 'w']:                
                            if m[6] != '' and m[5] != '':
                                #print(m)
                                country_id = get_id_country_db(m[1][0], sport_id)
                                if country_id != '':
                                    mm = [country_id, m[-1], datetime.strptime(m[0], '%d.%m.%Y %H:%M').strftime('%Y.%m.%d %H:%M'),
                                          m[2], m[3], m[6], float(m[5]), m[12], m[13], m[14], m[15], m[16]]
                                    #print(mm)
                                    load_db(mm)
                                    add_h2h(m[-1], m[13], m[14])
                                    
                                    # Обновляем tip2
                                    if m[17] != '':
                                        text_execute = f"""UPDATE event SET tip2='{m[17]}', tip2_odd='{m[18]}' WHERE event_id='{m[-1]}';"""
                                        update_line_db('event', text_execute)
                                    
                                    # Обновляем tip3
                                    if m[19] != '':
                                        text_execute = f"""UPDATE event SET tip3='{m[19]}', tip3_odd='{m[20]}' WHERE event_id='{m[-1]}';"""
                                        update_line_db('event', text_execute)
                                    
                    
                if m[-1] in sp_ids:
                    text_execute = f"""UPDATE event SET shortname_home="{m[13]}", shortname_away="{m[14]}", image_home="{m[15]}", image_away="{m[16]}"
                                WHERE event_id="{m[-1]}";"""
                
                    update_line_db('event', text_execute)
                    update_h2h(m[-1], m[13], m[14])
                    
                    # Обновляем tip2
                    if m[17] != '':
                        text_execute = f"""UPDATE event SET tip2='{m[17]}', tip2_odd='{m[18]}' WHERE event_id='{m[-1]}';"""
                        update_line_db('event', text_execute)
                    
                    # Обновляем tip3
                    if m[19] != '':
                        text_execute = f"""UPDATE event SET tip3='{m[19]}', tip3_odd='{m[20]}' WHERE event_id='{m[-1]}';"""
                        update_line_db('event', text_execute)
                        
            print('Updated results...')
            # Обновление результата завершенных матчей таблицы event
            # =========================================================
            all_spisok_matchey_close = [] # список завершенных матчей
            for m in all_spisok_matchey:                
                # Если матч завершен, то пополняем список соответствующих матчей
                if m[10] in sp_close_match_status:
                    if m[-1] in sp_ids:
                        
                        # Обновляем результаты прогноза
                        tip_result = ''
                        m[6] = get_tip_in_db('event', m[-1])
                        if m[10] in ['10','11']:
                            if sport_id == 3:
                                if m[6] == 'W1' or m[6] == 'W2':
                                    try:
                                        if int(m[7])>int(m[8]):
                                            if m[6] == 'W1':
                                                tip_result = 'won'
                                            if m[6] == 'W2':
                                                tip_result = 'loss'
                                                
                                        if int(m[7])<int(m[8]):
                                            if m[6] == 'W2':
                                                tip_result = 'won'
                                            if m[6] == 'W1':
                                                tip_result = 'loss'
                                        if int(m[7])==int(m[8]):
                                            tip_result = 'loss'
                                    except:
                                        pass
                            
                                elif m[6] == 'W1X' or m[6] == 'WX2':
                                    tip_result = 'won'
                                else:
                                    tip_result = 'loss'
                            
                            else:
                                if m[6] == 'W1X' or m[6] == 'WX2':
                                    tip_result = 'won'                        
                                else:
                                    tip_result = 'loss'
                        else:
                            if m[6] == 'W1' or m[6] == 'W2':
                                try:
                                    if int(m[7])>int(m[8]):
                                        if m[6] == 'W1':
                                            tip_result = 'won'
                                        if m[6] == 'W2':
                                            tip_result = 'loss'
                                            
                                    if int(m[7])<int(m[8]):
                                        if m[6] == 'W2':
                                            tip_result = 'won'
                                        if m[6] == 'W1':
                                            tip_result = 'loss'
                                    if int(m[7])==int(m[8]):
                                        tip_result = 'loss'
                                except:
                                    pass
                            
                            if m[6] == 'W1X' or m[6] == 'WX2':
                                try:
                                    if m[6] == 'W1X':
                                        if int(m[7])>=int(m[8]):                            
                                            tip_result = 'won'
                                        else:
                                            tip_result = 'loss'
                                    if m[6] == 'WX2':
                                        if int(m[7])<=int(m[8]):                            
                                            tip_result = 'won'
                                        else:
                                            tip_result = 'loss'
                                except:
                                    pass
                        
                        dan = [m[-1], sl_stage[m[10]], m[7], m[8], tip_result]
                        text_execute = f"""UPDATE event
                                        SET stage="{dan[1]}", tip_result="{dan[4]}", result_home="{dan[2]}", result_away="{dan[3]}"
                                        WHERE event_id="{dan[0]}";"""
                        
                        update_line_db('event', text_execute)                        
                        
                        
                        
                        # ============================================================
                        # Обновление результатов tip2, tip3
                        # ============================================================
                        tip2_rezult = ''
                        tip3_rezult = ''
                        
                        text_execute =  f"""SELECT * FROM event WHERE event_id='{m[-1]}'"""
                        sp_events = get_lines_db(text_execute)     
                        m_db = sp_events[0]                        
                        #print(m_db)                        
                        
                        try:
                            # Сумма забитых мячей
                            sum_score = int(m[7])+int(m[8])
                            
                            # Обновление tip2
                            tip_2 = m_db['tip2']                            
                            if tip_2 != None:
                                if 'U' in tip_2:
                                    point = float(tip_2.replace('U','').strip())
                                    if point<sum_score:
                                        tip2_rezult = 'loss'
                                    else:
                                        tip2_rezult = 'won'
                                
                                if 'O' in tip_2:
                                    point = float(tip_2.replace('O','').strip())
                                    if point>sum_score:
                                        tip2_rezult = 'loss'
                                    else:
                                        tip2_rezult = 'won'                        
                        except:
                            pass
                        
                        try:
                            # Обновление tip3                        
                            tip_3 = m_db['tip3']                            
                            if 'yes' in tip_3:                            
                                if m[7] != '0' and m[8] != '0':
                                    tip3_rezult = 'won'                                
                                else:
                                    tip3_rezult = 'loss'
                            
                            if 'no' in tip_3:
                                if m[7] == '0' or m[8] == '0':
                                    tip3_rezult = 'won'                                
                                else:
                                    tip3_rezult = 'loss'                            
                               
                        except:
                            pass

                        text_execute = f"""UPDATE event SET tip2_result="{tip2_rezult}", tip3_result="{tip3_rezult}" WHERE event_id="{m[-1]}";"""                        
                        update_line_db('event', text_execute) 
            # =========================================================
            # =========================================================
            
            # Загружаем vip
            if i == '0':
                load_vop_matches(all_spisok_matchey, sport_id)           
            
    print('ok')


def main():    
    sl_sport = {1:'Football',
                3:'Basketball',
                4:'Hockey',
                5:'American football',
                6:'Baseball'}
    
    for sport in SPORTS:
        print(f"{sl_sport[sport]} Parsing matches...")    
        parse_sport(sport)
        print('*'*30)


    
if __name__ == "__main__":
    clear_old()
    #generate_slugs()    
    main()
    generate_slugs()
    update_vip_matches()
