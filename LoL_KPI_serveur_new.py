import cassiopeia as cass
import pandas as pd
import datetime as date_time
from datetime import date
import pygsheets
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import time
import sys
import gspread_dataframe as gd

#GSHEET
SCOPES = ['https://www.googleapis.com/auth/drive.file']
KEY_FILE_LOCATION = 'League of Legends Players KPI-4c94be3fe887.json'
VIEW_ID = '999281075'
SHEET_ID = '12MhBP_ajMMJYoPWw0DdeKzYk9G0HhlztmciYqCQrxww'
#API
RIOT_API = "RGAPI-5c65f586-f880-4d69-8e8f-58546ff1aa78"
DEFAULT_REGION = "EUW"
REGION = ['BR','EUNE','EUW','JP','KR','LAN','LAS','NA','OCE','TR','RU']
TIER = ['IRON','BRONZE','SILVER','GOLD','PLATINUM','DIAMOND','MASTER','GRANDMASTER']
#'MASTER','GRANDMASTER','CHALLENGER'
DIVISION = ['IV','III','II','I']
DATAFRAME=['nouveau_joueur','joueur_par_division']




def from_cassiopeia_to_dataframe(tier,div,ref_div,reg):

    df_players_csv = pd.DataFrame()
    df_players_gsheet = pd.DataFrame()
    players_list=[]
    df_KPI_players = pd.DataFrame()
    cpt_active=0
    cpt_active_5=0
    cpt_active_10=0
    cpt_active_20=0
    cpt_active_30=0
    cpt_active_50=0
    cpt_active_51=0
    cpt_all=0
    if tier!="MASTER" and tier!="GRANDMASTER"and tier!="CHALLENGER":
        try:
            league= cass.get_paginated_league_entries(queue=cass.Queue.ranked_solo_fives, tier = tier, division = div,region = reg) #classement              
            if (len(sys.argv)==3 and sys.argv[2]!="ALL") or (len(sys.argv)==4 and sys.argv[3]!="ALL"):
                for league_entry in league:
                    cpt_all=cpt_all+1
                    #ON TESTE L'ACTIVITE DU JOUEUR
                    cat=is_an_active_player(league_entry.summoner)
                    if cat==5:
                        cpt_active_5=cpt_active_5+1
                        cpt_active=cpt_active+1
                    elif cat==10:
                        cpt_active_10=cpt_active_10+1
                        cpt_active=cpt_active+1
                    elif cat==20:
                        cpt_active_20=cpt_active_20+1
                        cpt_active=cpt_active+1
                    elif cat==30:
                        cpt_active_30=cpt_active_30+1
                        cpt_active=cpt_active+1
                    elif cat==50:
                        cpt_active_50=cpt_active_50+1
                        cpt_active=cpt_active+1
                    elif cat==51:
                        cpt_active_51=cpt_active_51+1
                        cpt_active=cpt_active+1
                    #ON INCREMENTE DE DF AVEC LES DONNEES DE CHAQUES JOUEURS
                    df_players_csv = df_players_csv.append({"Tier ":str(league_entry.tier),"Division":str(league_entry.division),"Summoner":str(league_entry.summoner),"Inactive":str(league_entry.inactive),"Date":str(date.today())},ignore_index = True)
                #ON UPDATE LE DATAFRAME QUI CONTIENT LES COMPTEURS DE JOUEURS ACTIFS ET NON ACTIFS
                df_KPI_players = df_KPI_players.append({"Tier ":str(league_entry.tier),"Division":str(league_entry.division),"Date":str(date.today()),"Region":str(reg),"Nombre_total":str(cpt_all),"Nombre_actif":str(cpt_active),"Nombre_actif_5":str(cpt_active_5),"Nombre_actif_10":str(cpt_active_10),"Nombre_actif_20":str(cpt_active_20),"Nombre_actif_30":str(cpt_active_30),"Nombre_actif_50":str(cpt_active_50),"Nombre_actif_50+":str(cpt_active_51)},ignore_index = True)
                #ON ENREGISTRE LE CSV ET ON LE VIDE 
                #df_players_csv.to_csv(r'C:\Users\jérôme\Desktop\PROJETS DEV\Riot API\df_players_'+str(date.today())+'.csv',mode='a', header=False,index=True)           
                df_players_csv.drop(df_players_csv.index, inplace=True)
            else :
                cpt_all = len(league)
                df_KPI_players = df_KPI_players.append({"Tier ":str(league.tier),"Division":str(league.division),"Date":str(date.today()),"Region":str(reg),"Nombre_total":str(cpt_all)},ignore_index = True)
            #ON EXPORTE VERS GSHEET LES DONNEES DES KPI
            export_to_sheets(df_KPI_players,div,tier,ref_div,reg)
            export_to_sheets_historique(df_KPI_players)
        except KeyboardInterrupt:
            print("control-c  quit done")
            if (len(sys.argv)==3 and sys.argv[2]!="ALL") or (len(sys.argv)==4 and sys.argv[3]!="ALL"):
                #df_players_csv.to_csv(r'C:\Users\jérôme\Desktop\PROJETS DEV\Riot API\df_players_'+str(date.today())+'.csv',mode='a', header=False,index=True)
                df_KPI_players = df_KPI_players.append({"Tier ":str(league_entry.tier),"Division":str(league_entry.division),"Date":str(date.today()),"Region":str(reg),"Nombre_total":str(cpt_all),"Nombre_actif":str(cpt_active),"Nombre_actif_5":str(cpt_active_5),"Nombre_actif_10":str(cpt_active_10),"Nombre_actif_20":str(cpt_active_20),"Nombre_actif_30":str(cpt_active_30),"Nombre_actif_50":str(cpt_active_50),"Nombre_actif_50+":str(cpt_active_51)},ignore_index = True)
            else:
                df_KPI_players = df_KPI_players.append({"Tier ":str(league.tier),"Division":str(league.division),"Date":str(date.today()),"Region":str(reg),"Nombre_total":str(cpt_all)},ignore_index = True)
            #ON ENREGISTRE LE CSV ET ON LE VIDE 
            export_to_sheets(df_KPI_players,div,tier,ref_div,reg)
            export_to_sheets_historique(df_KPI_players)

        return
    elif tier ==  "MASTER":   
        league_master = cass.get_master_league(queue=cass.Queue.ranked_solo_fives,region = reg)
        if (len(sys.argv)==3 and sys.argv[2]!="ALL") or (len(sys.argv)==4 and sys.argv[3]!="ALL"):
            for league_entry in league_master:
                cpt_all=cpt_all+1
                cat=is_an_active_player(league_entry.summoner)
                if cat==5:
                    cpt_active_5=cpt_active_5+1
                    cpt_active=cpt_active+1
                elif cat==10:
                    cpt_active_10=cpt_active_10+1
                    cpt_active=cpt_active+1
                elif cat==20:
                    cpt_active_20=cpt_active_20+1
                    cpt_active=cpt_active+1
                elif cat==30:
                    cpt_active_30=cpt_active_30+1
                    cpt_active=cpt_active+1
                elif cat==50:
                    cpt_active_50=cpt_active_50+1
                    cpt_active=cpt_active+1
                elif cat==51:
                    cpt_active_51=cpt_active_51+1
                    cpt_active=cpt_active+1
                df_players_csv = df_players_csv.append({"Tier ":"MASTER","Division":"N/A","Summoner":str(league_entry.summoner),"Inactive":str(league_entry.inactive),"Date":str(date.today())},ignore_index = True)
                #ON UPDATE LE DATAFRAME QUI CONTIENT LES COMPTEURS DE JOUEURS ACTIFS ET NON ACTIFS
                df_KPI_players = df_KPI_players.append({"Tier ":"MASTER","Division":"N/A","Date":str(date.today()),"Region":str(reg),"Nombre_total":str(cpt_all),"Nombre_actif":str(cpt_active),"Nombre_actif_5":str(cpt_active_5),"Nombre_actif_10":str(cpt_active_10),"Nombre_actif_20":str(cpt_active_20),"Nombre_actif_30":str(cpt_active_30),"Nombre_actif_50":str(cpt_active_50),"Nombre_actif_50+":str(cpt_active_51)},ignore_index = True)
            #df_players_csv.to_csv(r'C:\Users\jérôme\Desktop\PROJETS DEV\Riot API\df_players_'+str(date.today())+'.csv',mode='a', header=False,index=True)           
            df_players_csv.drop(df_players_csv.index, inplace=True) 
        else :
            cpt_all = len(league_master)
            df_KPI_players = df_KPI_players.append({"Tier ":str(league_master.tier),"Division":"N/A","Date":str(date.today()),"Region":str(reg),"Nombre_total":str(cpt_all)},ignore_index = True)
        #ON EXPORTE VERS GSHEET LES DONNEES DES KPI
        export_to_sheets(df_KPI_players,div,tier,ref_div,reg)
        export_to_sheets_historique(df_KPI_players)
        return
    elif tier ==  "GRANDMASTER":    
        league_grandmaster = cass.get_grandmaster_league(queue=cass.Queue.ranked_solo_fives,region = reg)
        if (len(sys.argv)==3 and sys.argv[2]!="ALL") or (len(sys.argv)==4 and sys.argv[3]!="ALL"):
            for league_entry in league_grandmaster:
                cpt_all=cpt_all+1
                cat=is_an_active_player(league_entry.summoner)
                if cat==5:
                    cpt_active_5=cpt_active_5+1
                    cpt_active=cpt_active+1
                elif cat==10:
                    cpt_active_10=cpt_active_10+1
                    cpt_active=cpt_active+1
                elif cat==20:
                    cpt_active_20=cpt_active_20+1
                    cpt_active=cpt_active+1
                elif cat==30:
                    cpt_active_30=cpt_active_30+1
                    cpt_active=cpt_active+1
                elif cat==50:
                    cpt_active_50=cpt_active_50+1
                    cpt_active=cpt_active+1
                elif cat==51:
                    cpt_active_51=cpt_active_51+1
                    cpt_active=cpt_active+1
                df_players_csv = df_players_csv.append({"Tier ":"GRANDMASTER","Division":"N/A","Summoner":str(league_entry.summoner),"Inactive":str(league_entry.inactive),"Date":str(date.today()) },ignore_index = True)
                #ON UPDATE LE DATAFRAME QUI CONTIENT LES COMPTEURS DE JOUEURS ACTIFS ET NON ACTIFS
                df_KPI_players = df_KPI_players.append({"Tier ":"GRANDMASTER","Division":"N/A","Date":str(date.today()),"Region":str(reg),"Nombre_total":str(cpt_all),"Nombre_actif":str(cpt_active),"Nombre_actif_5":str(cpt_active_5),"Nombre_actif_10":str(cpt_active_10),"Nombre_actif_20":str(cpt_active_20),"Nombre_actif_30":str(cpt_active_30),"Nombre_actif_50":str(cpt_active_50),"Nombre_actif_50+":str(cpt_active_51)},ignore_index = True)
            #df_players_csv.to_csv(r'C:\Users\jérôme\Desktop\PROJETS DEV\Riot API\df_players_'+str(date.today())+'.csv',mode='a', header=False,index=True)
            df_players_csv.drop(df_players_csv.index, inplace=True)
        else:
            cpt_all = len(league_grandmaster)
            df_KPI_players = df_KPI_players.append({"Tier ":str(league_grandmaster.tier),"Division":"N/A","Date":str(date.today()),"Region":str(reg),"Nombre_total":str(cpt_all)},ignore_index = True)
        export_to_sheets(df_KPI_players,div,tier,ref_div,reg)
        export_to_sheets_historique(df_KPI_players)
        return
    elif tier ==  "CHALLENGER":
        league_challenger=cass.get_challenger_league(queue=cass.Queue.ranked_solo_fives,region = reg)
        if (len(sys.argv)==3 and sys.argv[2]!="ALL") or (len(sys.argv)==4 and sys.argv[3]!="ALL"):
            for league_entry in league_challenger:
                cpt_all=cpt_all+1
                cat=is_an_active_player(league_entry.summoner)
                if cat==5:
                    cpt_active_5=cpt_active_5+1
                    cpt_active=cpt_active+1
                elif cat==10:
                    cpt_active_10=cpt_active_10+1
                    cpt_active=cpt_active+1
                elif cat==20:
                    cpt_active_20=cpt_active_20+1
                    cpt_active=cpt_active+1
                elif cat==30:
                    cpt_active_30=cpt_active_30+1
                    cpt_active=cpt_active+1
                elif cat==50:
                    cpt_active_50=cpt_active_50+1
                    cpt_active=cpt_active+1
                elif cat==51:
                    cpt_active_51=cpt_active_51+1
                    cpt_active=cpt_active+1
                df_players_csv = df_players_csv.append({"Tier ":"CHALLENGER","Division":"N/A","Summoner":str(league_entry.summoner),"Inactive":str(league_entry.inactive),"Date":str(date.today()) },ignore_index = True)
                #ON UPDATE LE DATAFRAME QUI CONTIENT LES COMPTEURS DE JOUEURS ACTIFS ET NON ACTIFS
                df_KPI_players = df_KPI_players.append({"Tier ":"CHALLENGER","Division":"N/A","Date":str(date.today()),"Region":str(reg),"Nombre_total":str(cpt_all),"Nombre_actif":str(cpt_active),"Nombre_actif_5":str(cpt_active_5),"Nombre_actif_10":str(cpt_active_10),"Nombre_actif_20":str(cpt_active_20),"Nombre_actif_30":str(cpt_active_30),"Nombre_actif_50":str(cpt_active_50),"Nombre_actif_50+":str(cpt_active_51)},ignore_index = True)
            #df_players_csv.to_csv(r'C:\Users\jérôme\Desktop\PROJETS DEV\Riot API\df_players_'+str(date.today())+'.csv',mode='a', header=False,index=True)           
            df_players_csv.drop(df_players_csv.index, inplace=True)
        else :
            cpt_all = len(league_grandmaster)
            df_KPI_players = df_KPI_players.append({"Tier ":str(league_challenger.tier),"Division":"N/A","Date":str(date.today()),"Region":str(reg),"Nombre_total":str(cpt_all)},ignore_index = True)
        export_to_sheets(df_KPI_players,div,tier,ref_div,reg)
        export_to_sheets_historique(df_KPI_players)
        return
    else:
        pass

def is_an_active_player(summ):
    date_day = date_time.datetime.now() + date_time.timedelta(-30)
    timestamp_date = int(date_time.datetime.timestamp(date_day))
    matches = cass.get_match_history(summoner=summ,begin_time=timestamp_date*1000)
    size = len(matches)
    if size <= 5:
        cat=5
    elif (size > 5) and (size <=10):
        cat=10
    elif (size > 10) and (size <=20):
        cat=20
    elif (size > 20) and (size <=30):
        cat=30
    elif (size > 30) and (size <=50):
        cat=50
    elif (size > 50):
        cat=51
    return cat

def export_to_sheets_historique(df):
    gc = pygsheets.authorize(service_file='League of Legends Players KPI-4c94be3fe887.json')
    sht = gc.open_by_key(SHEET_ID)
    wks = sht.worksheet_by_title('Historique')
    data =  wks.get_all_records()
    data_df = pd.DataFrame(data)
    df = df.append(data_df, ignore_index = True)
    wks.set_dataframe(df,'A1')

def export_to_sheets(df,div,tier,ref_div,reg):

    #REGION
    if reg == "BR":
        y = 12
    if reg == "EUNE":
        y = 21
    if reg == "EUW":
        y = 3
    if reg == "JP":
        y = 30
    if reg == "KR":
        y = 39
    if reg == "LAN":
        y = 48
    if reg == "LAS":
        y = 57
    if reg == "NA": 
        y = 66
    if reg == "OCE": 
        y = 75
    if reg == "TR":
        y = 84
    if reg == "RU":
        y = 93
    #TIER ET DIVISION
    if tier == "IRON":
        if div =="IV":
            x = 4 #G
        elif div =="III":
            x = 5 #F
        elif div =="II":
            x = 6 #F
        elif div =="I":
            x = 7 #D
    if tier == "BRONZE":
        if div =="IV":
            x = 8
        elif div =="III":
            x = 9
        elif div =="II":
            x = 10
        elif div =="I":
            x = 11
    if tier == "SILVER":
        if div =="IV":
            x = 12
        elif div =="III":
            x = 13
        elif div =="II":
            x = 14
        elif div =="I":
            x = 15
    if tier == "GOLD":
        if div =="IV":
            x = 16
        elif div =="III":
            x = 17
        elif div =="II":
            x = 18
        elif div =="I":
            x = 19
    if tier == "PLATINUM":
        if div =="IV":
            x = 20
        elif div =="III":
            x = 21
        elif div =="II":
            x = 22
        elif div =="I":
            x = 23
    if tier == "DIAMOND":
        if div =="IV":
            x = 24
        elif div =="III":
            x = 25
        elif div =="II":
            x = 26
        elif div =="I":
            x = 27
    if tier == "MASTER":
            x = 28
    if tier == "GRANDMASTER":
            x = 29
    if tier == "CHALLENGER":
            x = 30   

    if (len(sys.argv)==3 and sys.argv[2]!="ALL") or (len(sys.argv)==4 and sys.argv[3]!="ALL"):
        gc = pygsheets.authorize(service_file='League of Legends Players KPI-4c94be3fe887.json')
        sht = gc.open_by_key(SHEET_ID)
        wks = sht.worksheet_by_title('Source KPI')
        for i in df.index:
            wks.update_value((y,x),df['Nombre_total'][i])
            wks.update_value((y+1,x),df['Nombre_actif'][i])
            wks.update_value((y+2,x),df['Nombre_actif_5'][i])
            wks.update_value((y+3,x),df['Nombre_actif_10'][i])
            wks.update_value((y+4,x),df['Nombre_actif_20'][i])
            wks.update_value((y+5,x),df['Nombre_actif_30'][i])
            wks.update_value((y+6,x),df['Nombre_actif_50'][i])
            wks.update_value((y+7,x),df['Nombre_actif_50+'][i])
    else :
        gc = pygsheets.authorize(service_file='League of Legends Players KPI-4c94be3fe887.json')
        sht = gc.open_by_key(SHEET_ID)
        wks = sht.worksheet_by_title('Source KPI')
        for i in df.index:
            wks.update_value((y,x),df['Nombre_total'][i])

def initialize_reporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
    gc = pygsheets.authorize(service_file='League of Legends Players KPI-4c94be3fe887.json')



def initialisation():
    # This overrides the value set in your configuration/settings.
    conf = cass.get_default_config()
    conf["logging"]["print_calls"] = True
    #print(conf)
    conf["pipeline"]["RiotAPI"] = {
        "api_key" : RIOT_API,
        "request_error_handling" : {
            "500" : {
                "strategy": "exponential_backoff",
                "initial_backoff": 1.0,
                "backoff_factor": 2.0,
                "max_attempts": 4
            },
            "503" : {
                "strategy": "exponential_backoff",
                "initial_backoff": 1.0,
                "backoff_factor": 2.0,
                "max_attempts": 4
            }
        }
    }
    cass.apply_settings(conf)
    initialize_reporting()
    cass.set_default_region(DEFAULT_REGION)

def main():
    start_time = time.time()
    ref_div = 0
    initialisation()
    if len(sys.argv)==2:
        if sys.argv[1] != "ALL":
            pass
            """
            for tier in TIER:
                for div in DIVISION:                 
                        print("\n\n### tier : " + tier)
                        print("### division : " + div)
                        print("### region : " + sys.argv[2])
                        print("### KPI en cours de création")
                        from_cassiopeia_to_dataframe(tier,div,ref_div,sys.argv[1])
                        ref_div = ref_div+1
            """
        else:
            for reg in REGION:
                for tier in TIER:
                    for div in DIVISION:
                            print("\n\n### tier : " + tier)
                            print("### division : " + div)
                            print("### region : " + reg)
                            print("### KPI en cours de création")
                            from_cassiopeia_to_dataframe(tier,div,ref_div,reg)
                            ref_div = ref_div+1
    elif len(sys.argv)==4:
        if sys.argv[3] =="ALL":
            if sys.argv[2] !="MASTER" and sys.argv[2] !="GRANDMASTER" and sys.argv[2] !="CHALLENGER":
                for div in DIVISION:                 
                    print("\n\n### tier : " + sys.argv[2])
                    print("### division : " + div)
                    print("### region : " + sys.argv[1])
                    print("### KPI en cours de création")
                    from_cassiopeia_to_dataframe(sys.argv[2],div,ref_div,sys.argv[1])
                    ref_div = ref_div+1
            else :
                print("\n\n### tier : " + sys.argv[2])
                print("### division : N/A")
                print("### region : " + sys.argv[1])
                print("### KPI en cours de création")
                from_cassiopeia_to_dataframe(sys.argv[2],"n/a",ref_div,sys.argv[1])
        else:
            print("\n\n### tier : " + sys.argv[2])
            print("### division : " + sys.argv[3])
            print("### region : " + sys.argv[1])
            print("### KPI en cours de création")
            from_cassiopeia_to_dataframe(sys.argv[2],sys.argv[3],ref_div,sys.argv[1])

    elif len(sys.argv)==3:
        if sys.argv[2] =="MASTER" or sys.argv[2] =="GRANDMASTER" or sys.argv[2] =="CHALLENGER":
            print("\n\n### ier : " + sys.argv[2])
            print("### region : " + sys.argv[1])
            print("### KPI en cours de création")
            from_cassiopeia_to_dataframe(sys.argv[2],"n/a",ref_div,sys.argv[1])
        elif sys.argv[3]=="ALL":
            for tier in TIER:
                    for div in DIVISION:
                            print("\n\n### tier : " + tier)
                            print("### division : " + div)
                            print("### region : " + sys.argv[1])
                            print("### KPI en cours de création")
                            from_cassiopeia_to_dataframe(tier,div,ref_div,sys.argv[1])
                            ref_div = ref_div+1   
        else:
        	for div in DIVISION:
                print("\n\n### tier : " + sys.argv[2])
                print("### division : " + div)
                print("### region : " + sys.argv[1])
                print("### KPI en cours de création")
                from_cassiopeia_to_dataframe(sys.argv[2],div,ref_div,sys.argv[1])
                ref_div = ref_div+1

    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
    main()
