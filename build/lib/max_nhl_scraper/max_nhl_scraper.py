#Imports

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime 
import warnings
from typing import Dict, Union

warnings.filterwarnings('ignore')

#Constants
NHL_API_BASE_URL_1 = 'https://api-web.nhle.com/v1'

PLAY_BY_PLAY_ENDPOINT = f'{NHL_API_BASE_URL_1}/gamecenter/{{game_id}}/play-by-play'

SCHEDULE_ENDPOINT = f'{NHL_API_BASE_URL_1}/club-schedule-season/{{team_abbr}}/{{season}}'

SHIFT_REPORT_HOME_ENDPOINT = 'http://www.nhl.com/scores/htmlreports/{season}/TH{game_id}.HTM'
SHIFT_REPORT_AWAY_ENDPOINT = 'http://www.nhl.com/scores/htmlreports/{season}/TV{game_id}.HTM'

SHIFT_API_ENDPOINT = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={{game_id}}"


DEFAULT_SEASON = 20232024
DEFAULT_TEAM = "MTL"

NUMERICAL_COLUMNS = ['period', 'xCoord', 'yCoord', 'awayScore', 'homeScore', 'awaySOG','homeSOG', 'duration', 'event_player1_id', 'event_player2_id', 'event_player3_id', 'opposing_goalie_id', "game_id"]

CATEGORICAL_COLUMNS = ['homeTeamDefendingSide', 'typeDescKey', 'periodType',  'zoneCode', 'reason', 'shotType',  'typeCode', 'descKey', 'secondaryReason', "gameType", "venue", "season"]

def filter_players(players, side):
    if side is not None:
        side = side.lower()
        filter_condition = "is_home == 1" if side == "home" else "is_home == 0"
        players = players.query(filter_condition)
    return players

def str_to_sec(value):
    # Split the time value into minutes and seconds
    minutes, seconds = value.split(':')

    # Convert minutes and seconds to integers
    minutes = int(minutes)
    seconds = int(seconds)

    # Calculate the total seconds
    return minutes * 60 + seconds

def format_df(df):

    #Column names
    df.columns = [col.split('.')[-1] for col in df.columns]

    #Numerical columns
    df[NUMERICAL_COLUMNS] = df[NUMERICAL_COLUMNS].apply(pd.to_numeric, errors='coerce')

    #Category columns
    df[CATEGORICAL_COLUMNS] = df[CATEGORICAL_COLUMNS].astype("category")

    #Date and time cols
    df['startTimeUTC'] = pd.to_datetime(df['startTimeUTC'])#.dt.date to get only date


    # Apply the str_to_sec function to create the "timeInPeriod_s" column
    df[["timeInPeriod_s", 'timeRemaining_s']] = df[["timeInPeriod", 'timeRemaining']].map(str_to_sec)


    return df

def elapsed_time(df):

    # Calculate the elapsed time in seconds based on gameType and period
    df['elapsedTime'] = df['timeInPeriod_s'] + 60*(df['period'] - 1) * 20

    df.loc[(df['period'] >= 5) & (df["gameType"] != "playoffs"), 'elapsedTime'] = np.nan 

    return df

def add_missing_columns(df):
    cols_to_add = [
        "details.winningPlayerId", "details.losingPlayerId", "details.hittingPlayerId", "details.hitteePlayerId",
        "details.shootingPlayerId", "details.goalieInNetId", "details.playerId", "details.blockingPlayerId",
        "details.scoringPlayerId", "details.assist1PlayerId", "details.assist2PlayerId",
        "details.committedByPlayerId", "details.drawnByPlayerId", "details.servedByPlayerId",
        "situationCode", "typeCode", "sortOrder", "eventId", 'periodDescriptor.number'
    ] 

    for col in cols_to_add:
        if col not in df.columns:
            df[col] = np.nan
    return df

def format_columns(df):

    #Adding cols
    cols =  [
    "details.winningPlayerId", "details.losingPlayerId",
    "details.hittingPlayerId", "details.hitteePlayerId",
    "details.shootingPlayerId", "details.goalieInNetId",
    "details.playerId", "details.blockingPlayerId",
    "details.scoringPlayerId", "details.assist1PlayerId",
    "details.assist2PlayerId", "details.committedByPlayerId",
    "details.drawnByPlayerId", "details.servedByPlayerId",
    "situationCode", "typeCode", "sortOrder", "eventId", 'periodDescriptor.number']

    # Calculate the set difference to find missing columns
    columns_missing = set(cols) - set(df.columns)

    # Add missing columns with default values (e.g., None)
    for column in columns_missing:
        df[column] = np.nan

    #Faceoff
    df.loc[df["typeDescKey"] == 'faceoff', "event_player1_id"] = df["details.winningPlayerId"] #Winner
    df.loc[df["typeDescKey"] == 'faceoff', "event_player2_id"] = df["details.losingPlayerId"] #Loser

    #Hit
    df.loc[df["typeDescKey"] == 'hit', "event_player1_id"] = df["details.hittingPlayerId"] #Hitter
    df.loc[df["typeDescKey"] == 'hit', "event_player2_id"] = df["details.hitteePlayerId"] #Hittee

    #Missed shot & shot on goal
    df.loc[df["typeDescKey"].isin(['missed-shot', 'shot-on-goal', 'failed-shot-attempt']), "event_player1_id"] = df["details.shootingPlayerId"] #Shooter
    df.loc[df["typeDescKey"].isin(['missed-shot', 'shot-on-goal', 'failed-shot-attempt']), "event_player2_id"] = df["details.goalieInNetId"] #Goalie

    #Giveaway & Takeaway & Failed shot attempt (SO)
    ### Gotta investigate if failed penalty shot attempt is also a failed shot attempt ###
    df.loc[df["typeDescKey"].isin(['giveaway','takeaway']), "event_player1_id"] = df["details.playerId"] #Player

    #Blocked shot
    df.loc[df["typeDescKey"]== 'blocked-shot', "event_player1_id"] = df["details.shootingPlayerId"] #Shooter
    df.loc[df["typeDescKey"]== 'blocked-shot', "event_player2_id"] = df["details.blockingPlayerId"] #Blocker

    #Goal
    df.loc[df["typeDescKey"] == 'goal', "event_player1_id"] = df["details.scoringPlayerId"] #Goal-scorer
    df.loc[df["typeDescKey"] == 'goal', "event_player2_id"] = df["details.assist1PlayerId"] #1stPasser
    df.loc[df["typeDescKey"] == 'goal', "event_player3_id"] = df["details.assist2PlayerId"] #2ndPasser

    #Penalty
    df.loc[df["typeDescKey"] == 'penalty', "event_player1_id"] = df["details.committedByPlayerId"] #Penalized
    df.loc[df["typeDescKey"] == 'penalty', "event_player2_id"] = df["details.drawnByPlayerId"] #Drawer
    df.loc[df["typeDescKey"] == 'penalty', "event_player3_id"] = df["details.servedByPlayerId"] #Server

    #Opposing goalie
    df["opposing_goalie_id"] = df["details.goalieInNetId"]


    df.drop(["details.winningPlayerId", "details.losingPlayerId",
             "details.hittingPlayerId", "details.hitteePlayerId",
             "details.shootingPlayerId", "details.goalieInNetId",
             "details.playerId", "details.blockingPlayerId",
             "details.scoringPlayerId", "details.assist1PlayerId", "details.assist2PlayerId",
             "details.committedByPlayerId", "details.drawnByPlayerId", "details.servedByPlayerId",
             "situationCode", "typeCode", "sortOrder", "eventId", 'periodDescriptor.number', 'details.eventOwnerTeamId'
             ], axis=1, inplace=True)

    # Renaming columns
    df.columns = [col.split('.')[-1] for col in df.columns]

    # Converting columns to appropriate data types
    df[NUMERICAL_COLUMNS] = df[NUMERICAL_COLUMNS].apply(pd.to_numeric, errors='coerce')
    df[CATEGORICAL_COLUMNS] = df[CATEGORICAL_COLUMNS].astype("category")
    df['startTimeUTC'] = pd.to_datetime(df['startTimeUTC'])#.dt.date to get only date
    df[["timeInPeriod_s", 'timeRemaining_s']] = df[["timeInPeriod", 'timeRemaining']].map(str_to_sec)
    df = elapsed_time(df)
    return df

def add_event_players_info(df, rosters_df):
    p_df = rosters_df.copy()
    df = (df.merge(
        (p_df[['playerId', 'fullName','abbrev', 'positionCode']].rename(columns={'playerId':'event_player1_id',
                                                                      'fullName':'event_player1_fullName',
                                                                      'abbrev' : 'event_player1_team',
                                                                      'positionCode' : 'event_player1_position'})),
        on="event_player1_id",how="left"
    )
    .merge(
        (p_df[['playerId', 'fullName','abbrev', 'positionCode']].rename(columns={'playerId':'event_player2_id',
                                                                      'fullName':'event_player2_fullName',
                                                                      'abbrev' : 'event_player2_team',
                                                                      'positionCode' : 'event_player2_position'})),
        on="event_player2_id",how="left"
    )
    .merge(
        (p_df[['playerId', 'fullName','abbrev', 'positionCode']].rename(columns={'playerId':'event_player3_id',
                                                                      'fullName':'event_player3_fullName',
                                                                      'abbrev' : 'event_player3_team',
                                                                      'positionCode' : 'event_player3_position'})),
        on="event_player3_id",how="left"
    )
    .merge(
        (p_df[['playerId', 'fullName','abbrev', 'positionCode']].rename(columns={'playerId':'opposing_goalie_id',
                                                                      'fullName':'opposing_goalie_fullName',
                                                                      'abbrev' : 'opposing_goalie_team',
                                                                      'positionCode' : 'opposing_goalie_position'})),
        on="opposing_goalie_id",how="left"
    )
    )
    df["event_team"] = df["event_player1_team"]
    df.rename(columns={"typeDescKey" : "event"}, inplace=True)
    df["is_home"] = np.nan
    df.loc[df["event_team"] == df["home_abbr"],"is_home"] = 1
    df.loc[df["event_team"] == df["away_abbr"],"is_home"] = 0


    return df

def strength(df):

    ### FIX GAME STRENGTH ###

    ### THIS EXEMPLE scrape_game(2023020069) HAS WRONG GAME STRENGTH FOR GAME VS CAPS (5V5 IN OT) ###


    df['home_skaters'] = (~df[['home_on_position_1', 'home_on_position_2', 'home_on_position_3', 'home_on_position_4', 'home_on_position_5', 'home_on_position_6', 'home_on_position_7']].isin(['G', np.nan])).sum(axis=1)
    df['away_skaters'] = (~df[['away_on_position_1', 'away_on_position_2', 'away_on_position_3', 'away_on_position_4', 'away_on_position_5', 'away_on_position_6', 'away_on_position_7']].isin(['G', np.nan])).sum(axis=1)

    df["strength"] = np.where(df["event_team"] == df['home_abbr'], df['home_skaters'].astype(str) + 'v' + df['away_skaters'].astype(str), df['away_skaters'].astype(str) + 'v' + df['home_skaters'].astype(str))

    df.strength.replace({'0v0': None}, inplace=True)


    return df

def process_pbp(pbp, shifts_df, rosters_df, is_home=True):
    is_home = int(is_home)
    # print(is_home)
    place = 'home' if is_home else 'away'

    # players = rosters_df.query("is_home==@is_home").set_index('sweaterNumber')['playerId'].to_dict()
    # print(players)

    shifts_df = shifts_df.query("is_home==@is_home").query('duration_s > 0').copy()
    players_on = []

    # print(shifts_df)
    for _, row in pbp.iterrows():
        current_time = row['elapsedTime']
        if pd.isna(row['event_team']):
            players_on.append(np.nan)
        elif row['event'] == 'faceoff':
            # current_time = row['elapsedTime']
            # print(current_time)
            players_on_ice = shifts_df.query('startTime_s == @current_time')['playerId'].unique().tolist()
            
            # players_on_ice_2 = [players.get(int(item), int(item)) for item in players_on_ice]
            # print(players_on_ice)
            players_on.append(players_on_ice)
            
        # elif row['event'] == 'goal':
        #     players_on_ice = shifts_df.query('startTime_s < @current_time and endTime_s >= @current_time')['playerId'].unique().tolist()
            
        #     # players_on_ice_2 = [players.get(int(item), int(item)) for item in players_on_ice]
        #     # print(players_on_ice)
        #     players_on.append(players_on_ice)

        else:
            # current_time = row['elapsedTime']
            # print(current_time)
            # players_on_ice = shifts_df.query('startTime_s =< @current_time and endTime_s >= @current_time')['playerId'].unique().tolist()
            players_on_ice = shifts_df.query('startTime_s < @current_time and endTime_s >= @current_time')['playerId'].unique().tolist()
            # players_on_ice_2 = [players.get(int(item), int(item)) for item in players_on_ice]
            # print(players_on_ice)
            players_on.append(players_on_ice)
            if len(players_on_ice) > 7:
                print(row['game_id'],players_on_ice, current_time, row['event'])
    
    pbp[f'{place}_on'] = players_on

    max_list_length = pbp[f'{place}_on'].apply(lambda x: len(x) if isinstance(x, list) else 0).max()

    for i in range(max_list_length):
        pbp[f'{place}_on_id_{i+1}'] = np.nan

    for index, row in pbp.iterrows():
        values = row[f'{place}_on']
        if isinstance(values, list):
            for i, value in enumerate(values):
                pbp.at[index, f'{place}_on_id_{i+1}'] = value
                pbp.at[index, f'{place}_on_name_{i+1}'] = value
                pbp.at[index, f'{place}_on_position_{i+1}'] = value



    pbp[f"{place}_on_id_7"] = np.nan if f"{place}_on_id_7" not in pbp.columns else pbp[f"{place}_on_id_7"]

    pbp[f"{place}_on_name_1"], pbp[f"{place}_on_name_2"], pbp[f"{place}_on_name_3"], pbp[f"{place}_on_name_4"], pbp[f"{place}_on_name_5"], pbp[f"{place}_on_name_6"], pbp[f"{place}_on_name_7"] = pbp[f"{place}_on_id_1"], pbp[f"{place}_on_id_2"], pbp[f"{place}_on_id_3"], pbp[f"{place}_on_id_4"], pbp[f"{place}_on_id_5"], pbp[f"{place}_on_id_6"], pbp[f"{place}_on_id_7"]

    players_id = rosters_df.query("is_home==@is_home").set_index('playerId')['fullName'].to_dict()
    # Define the columns to be replaced
    columns_to_replace = [f"{place}_on_name_1", f"{place}_on_name_2", f"{place}_on_name_3", f"{place}_on_name_4", f"{place}_on_name_5", f"{place}_on_name_6", f"{place}_on_name_7"]

    # Use the replace method to replace player IDs with names
    pbp[columns_to_replace] = pbp[columns_to_replace].replace(players_id) 



    pbp[f"{place}_on_position_1"], pbp[f"{place}_on_position_2"], pbp[f"{place}_on_position_3"], pbp[f"{place}_on_position_4"], pbp[f"{place}_on_position_5"], pbp[f"{place}_on_position_6"], pbp[f"{place}_on_position_7"] = pbp[f"{place}_on_id_1"], pbp[f"{place}_on_id_2"], pbp[f"{place}_on_id_3"], pbp[f"{place}_on_id_4"], pbp[f"{place}_on_id_5"], pbp[f"{place}_on_id_6"], pbp[f"{place}_on_id_7"]

    players_id = rosters_df.query("is_home==@is_home").set_index('playerId')['positionCode'].to_dict()
    # Define the columns to be replaced
    columns_to_replace = [f"{place}_on_position_1", f"{place}_on_position_2", f"{place}_on_position_3", f"{place}_on_position_4", f"{place}_on_position_5", f"{place}_on_position_6", f"{place}_on_position_7"]

    # Use the replace method to replace player IDs with names
    pbp[columns_to_replace] = pbp[columns_to_replace].replace(players_id) 

    pbp.drop([f"{place}_on"], axis=1, inplace=True)
    pbp=pbp.loc[:, ~pbp.columns[::-1].duplicated()[::-1]]

    return pbp

#Fetch scripts

def fetch_play_by_play_json(game_id: int) -> Dict:
    """
    Connects to the NHL API to get the data for a given game.

    Args:
      game_id: Identifier ID for a given game.

    Returns:
      A JSON file with the information of the game.

    Raises:
      requests.exceptions.RequestException: If there's an issue with the request.
    """
    response = requests.get(PLAY_BY_PLAY_ENDPOINT.format(game_id=game_id))
    response.raise_for_status()  # Raise an error for bad responses.
    return response.json()

def fetch_team_schedule_json(team_abbr: str = DEFAULT_TEAM, season: int = DEFAULT_SEASON) -> Dict:
    """
    Connects to the NHL API to get the data for a given team's schedule.

    Args:
      team_abbr: Team abbreviation.
      season: Desired season in the format of {year_start}{year_end}.

    Returns:
      A JSON file with the schedule of a given team.

    Raises:
      requests.exceptions.RequestException: If there's an issue with the request.
    """
    response = requests.get(SCHEDULE_ENDPOINT.format(team_abbr=team_abbr, season=season))
    response.raise_for_status()
    return response.json()

def fetch_game_rosters(game_id: int, side: Union[str, None] = None, pbp_json: Union[Dict, None] = None) -> pd.DataFrame:
    """
    Fetches and processes rosters of both teams for a given game.

    Args:
      game_id: Identifier ID for a given game.
      side: To filter for the 'home' or away team. Default is None, meaning no filtering.
      pbp_json: JSON file of the Play-by-Play data of the game. Defaulted to None.

    Returns:
      A Pandas DataFrame with the rosters of both teams who played the game and information about the players.
    """
    
    pbp_json = fetch_play_by_play_json(game_id) if pbp_json is None else pbp_json


    players = pd.json_normalize(pbp_json.get("rosterSpots", [])).filter(['teamId', 'playerId', 'sweaterNumber', 'positionCode', 'headshot',
       'firstName.default', 'lastName.default']).rename(columns={'lastName.default':'lastName',
                  'firstName.default':'firstName'}).rename(columns={"id":"teamId","name":"team"})
    home_team, away_team = pd.json_normalize(pbp_json.get("homeTeam", [])), pd.json_normalize(pbp_json.get("awayTeam", []))
    teams = pd.concat([home_team.assign(is_home=1), away_team.assign(is_home=0)]).rename(columns={"id":"teamId", "name":"team"})
    players = players.merge(teams[["teamId", "abbrev", "is_home"]], on="teamId", how="left")
    players["fullName"] = players['firstName'] + " " + players['lastName']
    players["playerId"] = pd.to_numeric(players["playerId"])
    players["game_id"] = game_id

    return filter_players(players, side)

def fetch_html_shifts(game_id: int , season: Union[int, None] = None, pbp_json: Union[str, None] = None) -> pd.DataFrame: ### DEPPRECATED ###
    '''Retrives a Dataframe of the shifts actions for a given game.
    ##### Stolen from Patrick Bacon #####
    Args:
      game_id: Identifier ID for a given game.

    Returns:
      Dataframe of the shifts actions for a given game.

    Raises:
      IndexError: If this game has no shift data..
    '''

    
 
    season = f"{str(game_id)[:4]}{int(str(game_id)[:4]) + 1}" if season is None else season

    
    pbp_json = fetch_play_by_play_json(game_id) if pbp_json is None else pbp_json
    
    url = SHIFT_REPORT_HOME_ENDPOINT.format(season=season, game_id=str(game_id)[4:])
    page = (requests.get(url))
    soup = BeautifulSoup(page.content.decode('ISO-8859-1'), 'lxml', multi_valued_attributes = None, from_encoding='utf-8')
    found = soup.find_all('td', {'class':['playerHeading + border', 'lborder + bborder']})
    if len(found)==0:
        raise IndexError('This game has no shift data.')
    thisteam = soup.find('td', {'align':'center', 'class':'teamHeading + border'}).get_text()
    
    goalie_names = ['AARON DELL',
     'AARON SOROCHAN',
     'ADAM WERNER',
     'ADAM WILCOX',
     'ADIN HILL',
     'AL MONTOYA',
     'ALEX AULD',
     "ALEX D'ORIO",
     'ALEX LYON',
     'ALEX NEDELJKOVIC',
     'ALEX PECHURSKI',
     'ALEX SALAK',
     'ALEX STALOCK',
     'ALEXANDAR GEORGIEV',
     'ALEXEI MELNICHUK',
     'ALLEN YORK',
     'ANDERS LINDBACK',
     'ANDERS NILSSON',
     'ANDREI VASILEVSKIY',
     'ANDREW HAMMOND',
     'ANDREW RAYCROFT',
     'ANDREY MAKAROV',
     'ANGUS REDMOND',
     'ANTERO NIITTYMAKI',
     'ANTHONY STOLARZ',
     'ANTOINE BIBEAU',
     'ANTON FORSBERG',
     'ANTON KHUDOBIN',
     'ANTTI NIEMI',
     'ANTTI RAANTA',
     'ARTURS SILOVS',
     'ARTYOM ZAGIDULIN',
     'BEN BISHOP',
     'BEN SCRIVENS',
     'BEN WEXLER',
     'BRAD THIESSEN',
     'BRADEN HOLTBY',
     'BRANDON HALVERSON',
     'BRENT JOHNSON',
     'BRENT KRAHN',
     'BRETT LEONHARDT',
     'BRIAN BOUCHER',
     'BRIAN ELLIOTT',
     'BRIAN FOSTER',
     'BRYAN PITTON',
     'CALVIN HEETER',
     'CALVIN PETERSEN',
     'CALVIN PICKARD',
     'CAM TALBOT',
     'CAM WARD',
     'CAMERON JOHNSON',
     'CAREY PRICE',
     'CARTER HART',
     'CARTER HUTTON',
     'CASEY DESMITH',
     'CAYDEN PRIMEAU',
     'CEDRICK DESJARDINS',
     'CHAD JOHNSON',
     'CHARLIE LINDGREN',
     'CHET PICKARD',
     'CHRIS BECKFORD-TSEU',
     'CHRIS DRIEDGER',
     'CHRIS GIBSON',
     'CHRIS HOLT',
     'CHRIS MASON',
     'CHRIS OSGOOD',
     'COLE KEHLER',
     'COLLIN DELIA',
     'CONNOR HELLEBUYCK',
     'CONNOR INGRAM',
     'CONNOR KNAPP',
     'COREY CRAWFORD',
     'CORY SCHNEIDER',
     'CRAIG ANDERSON',
     'CRISTOBAL HUET',
     'CRISTOPHER NILSTORP',
     'CURTIS JOSEPH',
     'CURTIS MCELHINNEY',
     'CURTIS SANFORD',
     'DAN CLOUTIER',
     'DAN ELLIS',
     'DAN TURPLE',
     'DAN VLADAR',
     'DANIEL ALTSHULLER',
     'DANIEL LACOSTA',
     'DANIEL LARSSON',
     'DANIEL MANZATO',
     'DANIEL TAYLOR',
     'DANY SABOURIN',
     'DARCY KUEMPER',
     'DAREN MACHESNEY',
     'DAVID AEBISCHER',
     'DAVID AYRES',
     'DAVID LENEVEU',
     'DAVID RITTICH',
     'DAVID SHANTZ',
     'DENNIS ENDRAS',
     'DERECK BARIBEAU',
     'DEVAN DUBNYK',
     'DIMITRI PATZOLD',
     'DOMINIK HASEK',
     'DREW MACINTYRE',
     'DUSTIN BUTLER',
     'DUSTIN TOKARSKI',
     'DUSTYN ZENNER',
     'DWAYNE ROLOSON',
     'DYLAN FERGUSON',
     'DYLAN WELLS',
     'EAMON MCADAM',
     'EDDIE LACK',
     'EDWARD PASQUALE',
     'ELVIS MERZLIKINS',
     'EMIL LARMI',
     'ERIC COMRIE',
     'ERIC HARTZELL',
     'ERIC SEMBORSKI',
     'ERIK ERSBERG',
     'EVAN CORMIER',
     'EVAN FITZPATRICK',
     'EVGENI NABOKOV',
     'FELIX SANDSTROM',
     'FILIP GUSTAVSSON',
     'FRED BRATHWAITE',
     'FREDERIC CASSIVI',
     'FREDERIK ANDERSEN',
     'FREDRIK NORRENA',
     'GARRET SPARKS',
     'GAVIN MCHALE',
     'GERALD COLEMAN',
     'GILLES SENN',
     'HANNU TOIVONEN',
     'HARRI SATERI',
     'HENRIK KARLSSON',
     'HENRIK LUNDQVIST',
     'HUNTER MISKA',
     'IGOR BOBKOV',
     'IGOR SHESTERKIN',
     'IIRO TARKKI',
     'ILYA BRYZGALOV',
     'ILYA SAMSONOV',
     'ILYA SOROKIN',
     'IVAN PROSVETOV',
     'J-F BERUBE',
     'J.F. BERUBE',
     'JACK CAMPBELL',
     'JACOB MARKSTROM',
     'JAKE ALLEN',
     'JAKE OETTINGER',
     'JAMES REIMER',
     'JARED COREAU',
     'JAROSLAV HALAK',
     'JASON BACASHIHUA',
     'JASON KASDORF',
     'JASON LABARBERA',
     'JASON MISSIAEN',
     'JEAN-PHILIPPE LEVASSEUR',
     'JEAN-SEBASTIEN AUBIN',
     'JEAN-SEBASTIEN GIGUERE',
     'JEFF DESLAURIERS',
     'JEFF FRAZEE',
     'JEFF GLASS',
     'JEFF TYNI',
     'JEFF ZATKOFF',
     'JEREMY DUCHESNE',
     'JEREMY SMITH',
     'JEREMY SWAYMAN',
     'JHONAS ENROTH',
     'JIMMY HOWARD',
     'JOACIM ERIKSSON',
     'JOCELYN THIBAULT',
     'JOE CANNATA',
     'JOE FALLON',
     'JOEL MARTIN',
     'JOEY DACCORD',
     'JOEY MACDONALD',
     'JOHAN BACKLUND',
     'JOHAN GUSTAFSSON',
     'JOHAN HEDBERG',
     'JOHAN HOLMQVIST',
     'JOHN CURRY',
     'JOHN GIBSON',
     'JOHN GRAHAME',
     'JOHN MUSE',
     'JON GILLIES',
     'JON-PAUL ANDERSON',
     'JONAS GUSTAVSSON',
     'JONAS HILLER',
     'JONAS JOHANSSON',
     'JONATHAN BERNIER',
     'JONATHAN BOUTIN',
     'JONATHAN QUICK',
     'JONI ORTIO',
     'JOONAS KORPISALO',
     'JORDAN BINNINGTON',
     'JORDAN PEARCE',
     'JORDAN SIGALET',
     'JORDAN WHITE',
     'JORGE ALVES',
     'JOSE THEODORE',
     'JOSEF KORENAR',
     'JOSEPH WOLL',
     'JOSH HARDING',
     'JOSH TORDJMAN',
     'JUSSI RYNNAS',
     'JUSTIN KOWALKOSKI',
     'JUSTIN PETERS',
     'JUSTIN POGGE',
     'JUUSE SAROS',
     'JUUSO RIKSMAN',
     'KAAPO KAHKONEN',
     'KADEN FULCHER',
     'KARI LEHTONEN',
     'KARRI RAMO',
     'KASIMIR KASKISUO',
     'KEITH KINKAID',
     'KEN APPLEBY',
     'KENNETH APPLEBY',
     'KENT SIMPSON',
     'KEVIN BOYLE',
     'KEVIN LANKINEN',
     'KEVIN MANDOLESE',
     'KEVIN NASTIUK',
     'KEVIN POULIN',
     'KEVIN WEEKES',
     'KRISTERS GUDLEVSKIS',
     'KURTIS MUCHA',
     'LANDON BOW',
     'LARS JOHANSSON',
     'LAURENT BROSSOIT',
     'LELAND IRVING',
     'LINUS ULLMARK',
     'LOGAN THOMPSON',
     'LOUIS DOMINGUE',
     'LUKAS DOSTAL',
     'MACKENZIE BLACKWOOD',
     'MACKENZIE SKAPSKI',
     'MAGNUS HELLBERG',
     'MALCOLM SUBBAN',
     'MANNY FERNANDEZ',
     'MANNY LEGACE',
     'MARC CHEVERIE',
     'MARC DENIS',
     'MARC-ANDRE FLEURY',
     'MARCUS HOGBERG',
     'MAREK LANGHAMER',
     'MAREK MAZANEC',
     'MAREK SCHWARZ',
     'MARK DEKANICH',
     'MARK VISENTIN',
     'MARTIN BIRON',
     'MARTIN BRODEUR',
     'MARTIN GERBER',
     'MARTIN JONES',
     'MARTY TURCO',
     'MAT ROBSON',
     'MATHIEU CORBEIL',
     'MATHIEU GARON',
     'MATISS KIVLENIEKS',
     'MATT CLIMIE',
     'MATT DALTON',
     'MATT HACKETT',
     'MATT KEETLEY',
     'MATT MURRAY',
     'MATT VILLALTA',
     'MATT ZABA',
     'MATTHEW HEWITT',
     "MATTHEW O'CONNOR",
     'MAXIME LAGACE',
     'MICHAEL DIPIETRO',
     'MICHAEL GARTEIG',
     'MICHAEL HOUSER',
     'MICHAEL HUTCHINSON',
     'MICHAEL LEE',
     'MICHAEL LEIGHTON',
     'MICHAEL MCNIVEN',
     'MICHAEL MOLE',
     'MICHAEL MORRISON',
     'MICHAEL WALL',
     'MICHAL NEUVIRTH',
     'MIIKA WIIKMAN',
     'MIIKKA KIPRUSOFF',
     'MIKAEL TELLQVIST',
     'MIKE BRODEUR',
     'MIKE CONDON',
     'MIKE MCKENNA',
     'MIKE MURPHY',
     'MIKE SMITH',
     'MIKKO KOSKINEN',
     'MIROSLAV SVOBODA',
     'NATHAN DEOBALD',
     'NATHAN LAWSON',
     'NATHAN LIEUWEN',
     'NATHAN SCHOENFELD',
     'NICK ELLIS',
     'NIKLAS BACKSTROM',
     'NIKLAS LUNDSTROM',
     'NIKLAS SVEDBERG',
     'NIKLAS TREUTLE',
     'NIKOLAI KHABIBULIN',
     'NOLAN SCHAEFER',
     'OLIE KOLZIG',
     'ONDREJ PAVELEC',
     'OSCAR DANSK',
     'PASCAL LECLAIRE',
     'PAT CONACHER',
     'PATRICK KILLEEN',
     'PATRICK LALIME',
     'PAUL DEUTSCH',
     'PAVEL FRANCOUZ',
     'PEKKA RINNE',
     'PETER BUDAJ',
     'PETER MANNINO',
     'PETR MRAZEK',
     'PHEONIX COPLEY',
     'PHILIPP GRUBAUER',
     'PHILIPPE DESROSIERS',
     'RAY EMERY',
     'RETO BERRA',
     'RICHARD BACHMAN',
     'RICK DIPIETRO',
     'RIKU HELENIUS',
     'ROB LAURIE',
     'ROB ZEPP',
     'ROBB  TALLAS',
     'ROBBIE TALLAS',
     'ROBERT MAYER',
     'ROBERTO LUONGO',
     'ROBIN LEHNER',
     'ROMAN WILL',
     'RYAN LOWE',
     'RYAN MILLER',
     'RYAN MUNCE',
     'RYAN VINZ',
     'SAM BRITTAIN',
     'SAM MONTEMBEAULT',
     'SAMI AITTOKALLIO',
     'SAMUEL MONTEMBEAULT',
     'SCOTT CLEMMENSEN',
     'SCOTT DARLING',
     'SCOTT FOSTER',
     'SCOTT MUNROE',
     'SCOTT STAJCER',
     'SCOTT WEDGEWOOD',
     'SEBASTIEN CARON',
     'SEMYON VARLAMOV',
     'SERGEI BOBROVSKY',
     'SHAWN HUNWICK',
     'SPENCER KNIGHT',
     'SPENCER MARTIN',
     'STEFANOS LEKKAS',
     'STEVE MASON',
     'STEVE MICHALEK',
     'STEVE VALIQUETTE',
     'STUART SKINNER',
     'THATCHER DEMKO',
     'THOMAS FENTON',
     'THOMAS GREISS',
     'TIM THOMAS',
     'TIMO PIELMEIER',
     'TIMOTHY JR. THOMAS',
     'TOBIAS STEPHAN',
     'TODD FORD',
     'TOM MCCOLLUM',
     'TOMAS POPPERLE',
     'TOMAS VOKOUN',
     'TORRIE JUNG',
     'TRISTAN JARRY',
     'TROY GROSENICK',
     'TUUKKA RASK',
     'TY CONKLIN',
     'TYLER BUNZ',
     'TYLER PLANTE',
     'TYLER STEWART',
     'TYLER WEIMAN',
     'TYSON SEXSMITH',
     'UKKO-PEKKA LUUKKONEN',
     'VEINI VEHVILAINEN',
     'VESA TOSKALA',
     'VIKTOR FASTH',
     'VILLE HUSSO',
     'VITEK VANECEK',
     'WADE DUBIELEWICZ',
     'YANN DANIS',
     'ZACH FUCALE',
     'ZACH SIKICH',
     'ZACHARY FUCALE',
     'ZANE KALEMBA',
     'ZANE MCINTYRE']

    players = dict()

    for i in range(len(found)):
        line = found[i].get_text()
        if ', ' in line:
            name = line.split(',')
            number = name[0].split(' ')[0].strip()
            last_name =  name[0].split(' ')[1].strip()
            first_name = name[1].strip()
            full_name = first_name + " " + last_name
            players[full_name] = dict()
            players[full_name]['number'] = number
            players[full_name]['name'] = full_name
            players[full_name]['shifts'] = []
        else:
            players[full_name]['shifts'].extend([line])

    alldf = pd.DataFrame()

    for key in players.keys(): 
        length = int(len(np.array((players[key]['shifts'])))/5)
        df = pd.DataFrame(np.array((players[key]['shifts'])).reshape(length, 5)).rename(
        columns = {0:'shift_number', 1:'period', 2:'shift_start', 3:'shift_end', 4:'duration'})
        df = df.assign(name = players[key]['name'],
                      number = players[key]['number'],
                      team = thisteam,
                      venue = "home")
        alldf = pd.concat([alldf, df], ignore_index=True)
        
    home_shifts = alldf
    
    url = SHIFT_REPORT_AWAY_ENDPOINT.format(season=season, game_id=str(game_id)[4:])
    page = (requests.get(url))
    soup = BeautifulSoup(page.content.decode('ISO-8859-1'), 'lxml', multi_valued_attributes = None, from_encoding='utf-8')
    found = soup.find_all('td', {'class':['playerHeading + border', 'lborder + bborder']})
    thisteam = soup.find('td', {'align':'center', 'class':'teamHeading + border'}).get_text()

    players = dict()

    for i in range(len(found)):
        line = found[i].get_text()
        if ', ' in line:
            name = line.split(',')
            number = name[0].split(' ')[0].strip()
            last_name =  name[0].split(' ')[1].strip()
            first_name = name[1].strip()
            full_name = first_name + " " + last_name
            players[full_name] = dict()
            players[full_name]['number'] = number
            players[full_name]['name'] = full_name
            players[full_name]['shifts'] = []
        else:
            players[full_name]['shifts'].extend([line])

    alldf = pd.DataFrame()

    for key in players.keys(): 
        length = int(len(np.array((players[key]['shifts'])))/5)
        df = pd.DataFrame(np.array((players[key]['shifts'])).reshape(length, 5)).rename(
        columns = {0:'shift_number', 1:'period', 2:'shift_start', 3:'shift_end', 4:'duration'})
        df = df.assign(name = players[key]['name'],
                      number = players[key]['number'],
                      team = thisteam,
                      venue = "away")
        alldf = pd.concat([alldf, df], ignore_index=True)

    away_shifts = alldf
    
    all_shifts = pd.concat([home_shifts, away_shifts])
    
    all_shifts = all_shifts.assign(start_time = all_shifts.shift_start.str.split('/').str[0])
    
    all_shifts = all_shifts.assign(end_time = all_shifts.shift_end.str.split('/').str[0])
    
    #all_shifts = all_shifts[~all_shifts.end_time.str.contains('\xa0')]
    
    all_shifts.period = (np.where(all_shifts.period=='OT', 4, all_shifts.period)).astype(int)
    
    all_shifts = all_shifts.assign(end_time = np.where(~all_shifts.shift_end.str.contains('\xa0'), all_shifts.end_time,
              (np.where(
              (((pd.to_datetime(((60 * (all_shifts.start_time.str.split(':').str[0].astype(int))) + 
              (all_shifts.start_time.str.split(':').str[1].astype(int)) + 
                (60 * (all_shifts.duration.str.split(':').str[0].astype(int))).astype(int) +
              (all_shifts.duration.str.split(':').str[1].astype(int))).astype(int), unit = 's'))).dt.time).astype(str).str[3:].str[0]=='0',
              (((pd.to_datetime(((60 * (all_shifts.start_time.str.split(':').str[0].astype(int))) + 
              (all_shifts.start_time.str.split(':').str[1].astype(int)) + 
                (60 * (all_shifts.duration.str.split(':').str[0].astype(int))).astype(int) +
              (all_shifts.duration.str.split(':').str[1].astype(int))).astype(int), unit = 's'))).dt.time).astype(str).str[4:],
              (((pd.to_datetime(((60 * (all_shifts.start_time.str.split(':').str[0].astype(int))) + 
              (all_shifts.start_time.str.split(':').str[1].astype(int)) + 
                (60 * (all_shifts.duration.str.split(':').str[0].astype(int))).astype(int) +
              (all_shifts.duration.str.split(':').str[1].astype(int))).astype(int), unit = 's'))).dt.time).astype(str).str[4:]))))
    
    all_shifts['name'] = np.where(all_shifts['name'].str.contains('ALEXANDRE '), 
                                all_shifts.name.str.replace('ALEXANDRE ', 'ALEX '),
                                all_shifts['name'])
    
    all_shifts['name'] = np.where(all_shifts['name'].str.contains('ALEXANDER '), 
                                all_shifts.name.str.replace('ALEXANDER ', 'ALEX '),
                                all_shifts['name'])
    
    all_shifts['name'] = np.where(all_shifts['name'].str.contains('CHRISTOPHER '), 
                                all_shifts.name.str.replace('CHRISTOPHER ', 'CHRIS '),
                                all_shifts['name'])
    
    all_shifts = all_shifts.assign(name = 
    (np.where(all_shifts['name']== "ANDREI KASTSITSYN" , "ANDREI KOSTITSYN",
    (np.where(all_shifts['name']== "AJ GREER" , "A.J. GREER",
    (np.where(all_shifts['name']== "ANDREW GREENE" , "ANDY GREENE",
    (np.where(all_shifts['name']== "ANDREW WOZNIEWSKI" , "ANDY WOZNIEWSKI", 
    (np.where(all_shifts['name']== "ANTHONY DEANGELO" , "TONY DEANGELO",
    (np.where(all_shifts['name']== "BATES (JON) BATTAGLIA" , "BATES BATTAGLIA",
    (np.where(all_shifts['name'].isin(["BJ CROMBEEN", "B.J CROMBEEN", "BRANDON CROMBEEN", "B J CROMBEEN"]) , "B.J. CROMBEEN", 
    (np.where(all_shifts['name']== "BRADLEY MILLS" , "BRAD MILLS",
    (np.where(all_shifts['name']== "CAMERON BARKER" , "CAM BARKER", 
    (np.where(all_shifts['name']== "COLIN (JOHN) WHITE" , "COLIN WHITE",
    (np.where(all_shifts['name']== "CRISTOVAL NIEVES" , "BOO NIEVES",
    (np.where(all_shifts['name']== "CHRIS VANDE VELDE" , "CHRIS VANDEVELDE", 
    (np.where(all_shifts['name']== "DANNY BRIERE" , "DANIEL BRIERE",
    (np.where(all_shifts['name'].isin(["DAN CLEARY", "DANNY CLEARY"]) , "DANIEL CLEARY",
    (np.where(all_shifts['name']== "DANIEL GIRARDI" , "DAN GIRARDI", 
    (np.where(all_shifts['name']== "DANNY O'REGAN" , "DANIEL O'REGAN",
    (np.where(all_shifts['name']== "DANIEL CARCILLO" , "DAN CARCILLO", 
    (np.where(all_shifts['name']== "DAVID JOHNNY ODUYA" , "JOHNNY ODUYA", 
    (np.where(all_shifts['name']== "DAVID BOLLAND" , "DAVE BOLLAND", 
    (np.where(all_shifts['name']== "DENIS JR. GAUTHIER" , "DENIS GAUTHIER",
    (np.where(all_shifts['name']== "DWAYNE KING" , "DJ KING", 
    (np.where(all_shifts['name']== "EDWARD PURCELL" , "TEDDY PURCELL", 
    (np.where(all_shifts['name']== "EMMANUEL FERNANDEZ" , "MANNY FERNANDEZ", 
    (np.where(all_shifts['name']== "EMMANUEL LEGACE" , "MANNY LEGACE", 
    (np.where(all_shifts['name']== "EVGENII DADONOV" , "EVGENY DADONOV", 
    (np.where(all_shifts['name']== "FREDDY MODIN" , "FREDRIK MODIN", 
    (np.where(all_shifts['name']== "FREDERICK MEYER IV" , "FREDDY MEYER",
    (np.where(all_shifts['name']== "HARRISON ZOLNIERCZYK" , "HARRY ZOLNIERCZYK", 
    (np.where(all_shifts['name']== "ILJA BRYZGALOV" , "ILYA BRYZGALOV", 
    (np.where(all_shifts['name']== "JACOB DOWELL" , "JAKE DOWELL",
    (np.where(all_shifts['name']== "JAMES HOWARD" , "JIMMY HOWARD", 
    (np.where(all_shifts['name']== "JAMES VANDERMEER" , "JIM VANDERMEER",
    (np.where(all_shifts['name']== "JAMES WYMAN" , "JT WYMAN",
    (np.where(all_shifts['name']== "JOHN HILLEN III" , "JACK HILLEN",
    (np.where(all_shifts['name']== "JOHN ODUYA" , "JOHNNY ODUYA",
    (np.where(all_shifts['name']== "JOHN PEVERLEY" , "RICH PEVERLEY",
    (np.where(all_shifts['name']== "JONATHAN SIM" , "JON SIM",
    (np.where(all_shifts['name']== "JONATHON KALINSKI" , "JON KALINSKI",
    (np.where(all_shifts['name']== "JONATHAN AUDY-MARCHESSAULT" , "JONATHAN MARCHESSAULT", 
    (np.where(all_shifts['name']== "JOSEPH CRABB" , "JOEY CRABB",
    (np.where(all_shifts['name']== "JOSEPH CORVO" , "JOE CORVO", 
    (np.where(all_shifts['name']== "JOSHUA BAILEY" , "JOSH BAILEY",
    (np.where(all_shifts['name']== "JOSHUA HENNESSY" , "JOSH HENNESSY", 
    (np.where(all_shifts['name']== "JOSHUA MORRISSEY" , "JOSH MORRISSEY",
    (np.where(all_shifts['name']== "JEAN-FRANCOIS JACQUES" , "J-F JACQUES", 
    (np.where(all_shifts['name'].isin(["J P DUMONT", "JEAN-PIERRE DUMONT"]) , "J-P DUMONT", 
    (np.where(all_shifts['name']== "JT COMPHER" , "J.T. COMPHER",
    (np.where(all_shifts['name']== "KRISTOPHER LETANG" , "KRIS LETANG", 
    (np.where(all_shifts['name']== "KRYSTOFER BARCH" , "KRYS BARCH", 
    (np.where(all_shifts['name']== "KRYSTOFER KOLANOS" , "KRYS KOLANOS",
    (np.where(all_shifts['name']== "MARC POULIOT" , "MARC-ANTOINE POULIOT",
    (np.where(all_shifts['name']== "MARTIN ST LOUIS" , "MARTIN ST. LOUIS", 
    (np.where(all_shifts['name']== "MARTIN ST PIERRE" , "MARTIN ST. PIERRE",
    (np.where(all_shifts['name']== "MARTY HAVLAT" , "MARTIN HAVLAT",
    (np.where(all_shifts['name']== "MATTHEW CARLE" , "MATT CARLE", 
    (np.where(all_shifts['name']== "MATHEW DUMBA" , "MATT DUMBA",
    (np.where(all_shifts['name']== "MATTHEW BENNING" , "MATT BENNING", 
    (np.where(all_shifts['name']== "MATTHEW IRWIN" , "MATT IRWIN",
    (np.where(all_shifts['name']== "MATTHEW NIETO" , "MATT NIETO",
    (np.where(all_shifts['name']== "MATTHEW STAJAN" , "MATT STAJAN",
    (np.where(all_shifts['name']== "MAXIM MAYOROV" , "MAKSIM MAYOROV",
    (np.where(all_shifts['name']== "MAXIME TALBOT" , "MAX TALBOT", 
    (np.where(all_shifts['name']== "MAXWELL REINHART" , "MAX REINHART",
    (np.where(all_shifts['name']== "MICHAEL BLUNDEN" , "MIKE BLUNDEN",
    (np.where(all_shifts['name'].isin(["MICHAËL BOURNIVAL", "MICHAÃ\x8bL BOURNIVAL"]), "MICHAEL BOURNIVAL",
    (np.where(all_shifts['name']== "MICHAEL CAMMALLERI" , "MIKE CAMMALLERI", 
    (np.where(all_shifts['name']== "MICHAEL FERLAND" , "MICHEAL FERLAND", 
    (np.where(all_shifts['name']== "MICHAEL GRIER" , "MIKE GRIER",
    (np.where(all_shifts['name']== "MICHAEL KNUBLE" , "MIKE KNUBLE",
    (np.where(all_shifts['name']== "MICHAEL KOMISAREK" , "MIKE KOMISAREK",
    (np.where(all_shifts['name']== "MICHAEL MATHESON" , "MIKE MATHESON",
    (np.where(all_shifts['name']== "MICHAEL MODANO" , "MIKE MODANO",
    (np.where(all_shifts['name']== "MICHAEL RUPP" , "MIKE RUPP",
    (np.where(all_shifts['name']== "MICHAEL SANTORELLI" , "MIKE SANTORELLI", 
    (np.where(all_shifts['name']== "MICHAEL SILLINGER" , "MIKE SILLINGER",
    (np.where(all_shifts['name']== "MITCHELL MARNER" , "MITCH MARNER", 
    (np.where(all_shifts['name']== "NATHAN GUENIN" , "NATE GUENIN",
    (np.where(all_shifts['name']== "NICHOLAS BOYNTON" , "NICK BOYNTON",
    (np.where(all_shifts['name']== "NICHOLAS DRAZENOVIC" , "NICK DRAZENOVIC", 
    (np.where(all_shifts['name']== "NICKLAS BERGFORS" , "NICLAS BERGFORS",
    (np.where(all_shifts['name']== "NICKLAS GROSSMAN" , "NICKLAS GROSSMANN", 
    (np.where(all_shifts['name']== "NICOLAS PETAN" , "NIC PETAN", 
    (np.where(all_shifts['name']== "NIKLAS KRONVALL" , "NIKLAS KRONWALL",
    (np.where(all_shifts['name']== "NIKOLAI ANTROPOV" , "NIK ANTROPOV",
    (np.where(all_shifts['name']== "NIKOLAI KULEMIN" , "NIKOLAY KULEMIN", 
    (np.where(all_shifts['name']== "NIKOLAI ZHERDEV" , "NIKOLAY ZHERDEV",
    (np.where(all_shifts['name']== "OLIVIER MAGNAN-GRENIER" , "OLIVIER MAGNAN",
    (np.where(all_shifts['name']== "PAT MAROON" , "PATRICK MAROON", 
    (np.where(all_shifts['name'].isin(["P. J. AXELSSON", "PER JOHAN AXELSSON"]) , "P.J. AXELSSON",
    (np.where(all_shifts['name'].isin(["PK SUBBAN", "P.K SUBBAN"]) , "P.K. SUBBAN", 
    (np.where(all_shifts['name'].isin(["PIERRE PARENTEAU", "PIERRE-ALEX PARENTEAU", "PIERRE-ALEXANDRE PARENTEAU", "PA PARENTEAU", "P.A PARENTEAU", "P-A PARENTEAU"]) , "P.A. PARENTEAU", 
    (np.where(all_shifts['name']== "PHILIP VARONE" , "PHIL VARONE",
    (np.where(all_shifts['name']== "QUINTIN HUGHES" , "QUINN HUGHES",
    (np.where(all_shifts['name']== "RAYMOND MACIAS" , "RAY MACIAS",
    (np.where(all_shifts['name']== "RJ UMBERGER" , "R.J. UMBERGER",
    (np.where(all_shifts['name']== "ROBERT BLAKE" , "ROB BLAKE",
    (np.where(all_shifts['name']== "ROBERT EARL" , "ROBBIE EARL",
    (np.where(all_shifts['name']== "ROBERT HOLIK" , "BOBBY HOLIK",
    (np.where(all_shifts['name']== "ROBERT SCUDERI" , "ROB SCUDERI",
    all_shifts['name']))))))))))))))))))))))))))))))))))))))))))))))))))))))
    )))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))
    ))))))))))
    
    all_shifts['name'] = (np.where(all_shifts['name']== "RODNEY PELLEY" , "ROD PELLEY",
    (np.where(all_shifts['name']== "SIARHEI KASTSITSYN" , "SERGEI KOSTITSYN",
    (np.where(all_shifts['name']== "SIMEON VARLAMOV" , "SEMYON VARLAMOV", 
    (np.where(all_shifts['name']== "STAFFAN KRONVALL" , "STAFFAN KRONWALL",
    (np.where(all_shifts['name']== "STEVEN REINPRECHT" , "STEVE REINPRECHT",
    (np.where(all_shifts['name']== "TJ GALIARDI" , "T.J. GALIARDI",
    (np.where(all_shifts['name']== "TJ HENSICK" , "T.J. HENSICK",
    (np.where(all_shifts['name'].isin(["TJ OSHIE", "T.J OSHIE"]) , "T.J. OSHIE", 
    (np.where(all_shifts['name']== "TOBY ENSTROM" , "TOBIAS ENSTROM", 
    (np.where(all_shifts['name']== "TOMMY SESTITO" , "TOM SESTITO",
    (np.where(all_shifts['name']== "VACLAV PROSPAL" , "VINNY PROSPAL",
    (np.where(all_shifts['name']== "VINCENT HINOSTROZA" , "VINNIE HINOSTROZA",
    (np.where(all_shifts['name']== "WILLIAM THOMAS" , "BILL THOMAS",
    (np.where(all_shifts['name']== "ZACHARY ASTON-REESE" , "ZACH ASTON-REESE",
    (np.where(all_shifts['name']== "ZACHARY SANFORD" , "ZACH SANFORD",
    (np.where(all_shifts['name']== "ZACHERY STORTINI" , "ZACK STORTINI",
    (np.where(all_shifts['name']== "MATTHEW MURRAY" , "MATT MURRAY",
    (np.where(all_shifts['name']== "J-SEBASTIEN AUBIN" , "JEAN-SEBASTIEN AUBIN",
    (np.where(all_shifts['name'].isin(["J.F. BERUBE", "JEAN-FRANCOIS BERUBE"]) , "J-F BERUBE", 
    (np.where(all_shifts['name']== "JEFF DROUIN-DESLAURIERS" , "JEFF DESLAURIERS", 
    (np.where(all_shifts['name']== "NICHOLAS BAPTISTE" , "NICK BAPTISTE",
    (np.where(all_shifts['name']== "OLAF KOLZIG" , "OLIE KOLZIG",
    (np.where(all_shifts['name']== "STEPHEN VALIQUETTE" , "STEVE VALIQUETTE",
    (np.where(all_shifts['name']== "THOMAS MCCOLLUM" , "TOM MCCOLLUM",
    (np.where(all_shifts['name']== "TIMOTHY JR. THOMAS" , "TIM THOMAS",
    (np.where(all_shifts['name']== "TIM GETTINGER" , "TIMOTHY GETTINGER",
    (np.where(all_shifts['name']== "NICHOLAS SHORE" , "NICK SHORE",
    (np.where(all_shifts['name']== "T.J. TYNAN" , "TJ TYNAN",
    (np.where(all_shifts['name']== "ALEXIS LAFRENI?RE" , "ALEXIS LAFRENIÈRE",
    (np.where(all_shifts['name']== "ALEXIS LAFRENIERE" , "ALEXIS LAFRENIÈRE", 
    (np.where(all_shifts['name']== "ALEXIS LAFRENIÃRE" , "ALEXIS LAFRENIÈRE",
    (np.where(all_shifts['name']== "TIM STUTZLE" , "TIM STÜTZLE",
    (np.where(all_shifts['name']== "TIM ST?TZLE" , "TIM STÜTZLE",
    (np.where(all_shifts['name']== "TIM STÃTZLE" , "TIM STÜTZLE",
    (np.where(all_shifts['name']== "EGOR SHARANGOVICH" , "YEGOR SHARANGOVICH",
    (np.where(all_shifts['name']== "CALLAN FOOTE" , "CAL FOOTE",
    (np.where(all_shifts['name']== "MATTIAS JANMARK-NYLEN" , "MATTIAS JANMARK",
    (np.where(all_shifts['name']== "JOSH DUNNE" , "JOSHUA DUNNE",all_shifts['name'])))))))))))))))))))))))))))))))))))))))))))
    )))))))))))))))))))))))))))))))))
    
    
    all_shifts = all_shifts.assign(end_time = np.where(pd.to_datetime(all_shifts.start_time).dt.time > pd.to_datetime(all_shifts.end_time).dt.time, '20:00', all_shifts.end_time),
                                  goalie = np.where(all_shifts.name.isin(goalie_names), 1, 0))
    
    all_shifts = all_shifts.merge(all_shifts.groupby(['team', 'period'])['goalie'].sum().reset_index().rename(columns = {'goalie':'period_gs'}))
    
    # Implement fix for goalies: Goalies who showed up late in the period and were the only goalie to play have their start time re-set to 0:00. 
    
    all_shifts = all_shifts.assign(start_time = np.where((all_shifts.goalie==1) & (all_shifts.start_time!='0:00') & (all_shifts.period_gs==1), '0:00', all_shifts.start_time))
    
    all_shifts = all_shifts.assign(end_time = np.where(
    (pd.to_datetime(all_shifts.start_time).dt.time < datetime(2021, 6, 10, 18, 0, 0).time()) & 
    (all_shifts.period!=3) & (all_shifts.period!=4) & 
    (all_shifts.goalie==1) &
    (all_shifts.period_gs==1),
    '20:00', all_shifts.end_time))
    
    all_shifts = all_shifts.assign(end_time = np.where(
    (pd.to_datetime(all_shifts.start_time).dt.time < datetime(2021, 6, 10, 13, 0, 0).time()) & 
    (all_shifts.period!=4) &
    (all_shifts.goalie==1) &
    (all_shifts.period_gs==1),
    '20:00', all_shifts.end_time))
    
    myshifts = all_shifts
    
    myshifts.start_time = myshifts.start_time.str.strip()
    myshifts.end_time = myshifts.end_time.str.strip()

    changes_on = myshifts.groupby(['team', 'period', 'start_time']).agg(
        on = ('name', ', '.join),
        on_numbers = ('number', ', '.join),
        number_on = ('name', 'count')
    ).reset_index().rename(columns = {'start_time':'time'}).sort_values(by = ['team', 'period', 'time'])
    
    changes_off = myshifts.groupby(['team', 'period', 'end_time']).agg(
        off = ('name', ', '.join),
        off_numbers = ('number', ', '.join),
        number_off = ('name', 'count')
    ).reset_index().rename(columns = {'end_time':'time'}).sort_values(by = ['team', 'period', 'time'])
    
    all_on = changes_on.merge(changes_off, on = ['team', 'period', 'time'], how = 'left')
    off_only = changes_off.merge(changes_on, on = ['team', 'period', 'time'], how = 'left', indicator = True)[
    changes_off.merge(changes_on, on = ['team', 'period', 'time'], how = 'left', indicator = True)['_merge']!='both']
    full_changes = pd.concat([all_on, off_only]).sort_values(by = ['period', 'time']).drop(columns = ['_merge'])
    
    full_changes['period_seconds'] = full_changes.time.str.split(':').str[0].astype(int) * 60 + full_changes.time.str.split(':').str[1].astype(int)

    full_changes['game_seconds'] = (np.where(full_changes.period<5, 
                                   (((full_changes.period - 1) * 1200) + full_changes.period_seconds),
                          3900))
    
    full_changes = full_changes.assign(team = np.where(full_changes.team=='CANADIENS MONTREAL', 'MONTREAL CANADIENS', full_changes.team))
    full_changes = full_changes.assign(team = np.where(full_changes.team=='MONTRÃAL CANADIENS', 'MONTREAL CANADIENS', full_changes.team))
    
    full_changes.reset_index(drop = True, inplace=True)#.drop(columns = ['time', 'period_seconds']) 

    full_changes['is_home'] = 0
    full_changes.loc[full_changes['team'].str.contains(pbp_json['homeTeam']['name'], case=False), 'is_home'] = 1
    return full_changes

def fetch_api_shifts(game_id, pbp_json=None):
    '''
    Fetches shifts data from the NHL API and returns a DataFrame with the data.
    ----
    :param game_id: The game ID of the game to fetch shifts for.
    :param pbp_json: The play-by-play JSON for the game. If not provided, it will be fetched from the API.
    :return: A DataFrame containing the shifts data for the game.
    '''


    # Fetch play-by-play data
    pbp_json = fetch_play_by_play_json(game_id) if pbp_json is None else pbp_json

    home_team_abbrev = pbp_json["homeTeam"]["abbrev"]
    # away_team_abbrev = pbp_json["awayTeam"]["abbrev"]

    # Fetch shifts data from the API
    shifts_data = requests.get(SHIFT_API_ENDPOINT.format(game_id=game_id)).json()['data']

    # Create a DataFrame and perform data transformations
    shift_df = pd.json_normalize(shifts_data)
    shift_df = shift_df.drop(columns=['id', 'detailCode', 'eventDescription', 'eventDetails', 'eventNumber', 'typeCode'])
    shift_df['fullName'] = shift_df['firstName'] + " " + shift_df['lastName']
    shift_df['duration_s'] = shift_df['duration'].fillna('00:00').apply(str_to_sec)
    shift_df['startTime_s'] = shift_df['startTime'].apply(str_to_sec) + 60 * (shift_df['period'] - 1) * 20
    shift_df['endTime_s'] = shift_df['endTime'].apply(str_to_sec) + 60 * (shift_df['period'] - 1) * 20
    shift_df['teamAbbrev'] = shift_df['teamAbbrev'].str.strip()
    shift_df['is_home'] = np.where(shift_df['teamAbbrev'] == home_team_abbrev, 1, 0)

    # Filter and select relevant columns
    columns_to_select = [
        'playerId', 'fullName', 'teamAbbrev', 'startTime_s', 'endTime_s', 'duration_s',
        'period', 'startTime', 'endTime', 'duration', 'firstName', 'lastName',
        'teamName', 'teamId', 'shiftNumber', 'gameId', 'hexValue', 'is_home'
    ]
    shift_df = shift_df[columns_to_select]

    shift_df["type"] = "OTF"

    faceoffs = (pd.json_normalize(pbp_json["plays"])
                .query('typeDescKey=="faceoff"')
                .filter(['timeInPeriod','homeTeamDefendingSide', 'details.xCoord','details.zoneCode', 'period'])
                .assign(current_time = lambda x: x['timeInPeriod'].apply(str_to_sec) +20*60* (x['period']-1))
                .drop(columns=['timeInPeriod', 'period']))

    

    for _, shift in shift_df.iterrows():

        time = shift["startTime_s"]
        if time in faceoffs["current_time"].values:
            matching_faceoffs = faceoffs.query("current_time == @time")
            zoneCode = matching_faceoffs["details.zoneCode"].values[0]
            homeTeamZone = matching_faceoffs["homeTeamDefendingSide"].values[0]
            xCoord = matching_faceoffs["details.xCoord"].values[0]



            if zoneCode == "N":
                shift_df.at[_, "type"] = "NZF"
            elif (
                homeTeamZone == "left" and shift["is_home"] == 1 and xCoord < 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 1 and xCoord > 0
            ) or (
                homeTeamZone == "left" and shift["is_home"] == 0 and xCoord > 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 0 and xCoord < 0
            ):
                shift_df.at[_, "type"] = "DZF"
            elif (
                homeTeamZone == "left" and shift["is_home"] == 1 and xCoord > 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 1 and xCoord < 0
            ) or (
                homeTeamZone == "left" and shift["is_home"] == 0 and xCoord < 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 0 and xCoord > 0
            ):
                shift_df.at[_, "type"] = "OZF"
        else:
            shift_df.at[_, "type"] = "OTF"

    shift_df['date'] = pbp_json['gameDate']
    shift_df['season'] = pbp_json['season']
    shift_df['gameType'] = game_id
    

    return shift_df

def fetch_html_shifts2(game_id=2023020069, season=None, pbp_json=None):
    ''' 
    Fetches shifts data from the NHL API and returns a DataFrame with the data.
    ----
    :param game_id: The game ID of the game to fetch shifts for.
    :param season: The season of the game. If not provided, it will be fetched from the API.
    :param pbp_json: The play-by-play JSON for the game. If not provided, it will be fetched from the API.
    :return: A DataFrame containing the shifts data for the game.
    '''

    pbp_json = fetch_play_by_play_json(game_id) if pbp_json is None else pbp_json
    rosters = fetch_game_rosters(game_id)

    season = f"{str(game_id)[:4]}{int(str(game_id)[:4]) + 1}" if season is None else season


    ### HOME SHIFTS ###
    url = SHIFT_REPORT_HOME_ENDPOINT.format(season=season, game_id=str(game_id)[4:])
    page = (requests.get(url))
    soup = BeautifulSoup(page.content.decode('ISO-8859-1'), 'lxml', multi_valued_attributes = None, from_encoding='utf-8')
    found = soup.find_all('td', {'class':['playerHeading + border', 'lborder + bborder']})
    if len(found)==0:
        raise IndexError('This game has no shift data.')
    thisteam = soup.find('td', {'align':'center', 'class':'teamHeading + border'}).get_text()
    

    players = dict()
    for i in range(len(found)):
        line = found[i].get_text()
        if ', ' in line:
            name = line.split(',')
            number = name[0].split(' ')[0].strip()
            last_name =  name[0].split(' ')[1].strip()
            first_name = name[1].strip()
            full_name = first_name + " " + last_name
            players[full_name] = dict()
            players[full_name]['number'] = number
            players[full_name]['name'] = full_name
            players[full_name]['shifts'] = []
        else:
            players[full_name]['shifts'].extend([line])

    alldf = pd.DataFrame()

    for key in players.keys(): 
        length = int(len(np.array((players[key]['shifts'])))/5)
        df = pd.DataFrame(np.array((players[key]['shifts'])).reshape(length, 5)).rename(
        columns = {0:'shift_number', 1:'period', 2:'shift_start', 3:'shift_end', 4:'duration'})
        df = df.assign(name = players[key]['name'],
                      sweaterNumber = int(players[key]['number']),
                      team = thisteam,
                      is_home = 1)
        alldf = pd.concat([alldf, df], ignore_index=True)
        
    home_shifts = alldf

    ### AWAY SHIFTS ###
    url = SHIFT_REPORT_AWAY_ENDPOINT.format(season=season, game_id=str(game_id)[4:])
    page = (requests.get(url))
    soup = BeautifulSoup(page.content.decode('ISO-8859-1'), 'lxml', multi_valued_attributes = None, from_encoding='utf-8')
    found = soup.find_all('td', {'class':['playerHeading + border', 'lborder + bborder']})
    if len(found)==0:
        raise IndexError('This game has no shift data.')
    thisteam = soup.find('td', {'align':'center', 'class':'teamHeading + border'}).get_text()
    

    players = dict()
    for i in range(len(found)):
        line = found[i].get_text()
        if ', ' in line:
            name = line.split(',')
            number = name[0].split(' ')[0].strip()
            last_name =  name[0].split(' ')[1].strip()
            first_name = name[1].strip()
            full_name = first_name + " " + last_name
            players[full_name] = dict()
            players[full_name]['number'] = number
            players[full_name]['name'] = full_name
            players[full_name]['shifts'] = []
        else:
            players[full_name]['shifts'].extend([line])

    alldf = pd.DataFrame()

    for key in players.keys(): 
        length = int(len(np.array((players[key]['shifts'])))/5)
        df = pd.DataFrame(np.array((players[key]['shifts'])).reshape(length, 5)).rename(
        columns = {0:'shift_number', 1:'period', 2:'shift_start', 3:'shift_end', 4:'duration'})
        df = df.assign(name = players[key]['name'],
                      sweaterNumber = int(players[key]['number']),
                      team = thisteam,
                      is_home = 0)
        alldf = pd.concat([alldf, df], ignore_index=True)
        
    away_shifts = alldf

    ### MERGE SHIFTS ###
    all_shifts = (pd.concat([home_shifts, away_shifts], ignore_index=True)
                  .drop(columns=['name', 'team'])
                  .merge(rosters, how='left', on=['sweaterNumber', 'is_home']))


    all_shifts[['startTime', 'startTime_remaning']] = all_shifts['shift_start'].str.split(' / ', expand=True)

    # Split 'shift_end' column into two columns
    all_shifts[['endTime', 'endTime_remaning']] = all_shifts['shift_end'].str.split(' / ', expand=True)    

    all_shifts = all_shifts.drop(columns=[ 'startTime_remaning',  'endTime_remaning', 'shift_start', 'shift_end']).replace({'OT':4})

    all_shifts['period'] = all_shifts['period'].astype(int)
    all_shifts['duration_s'] = all_shifts['duration'].fillna('00:00').apply(str_to_sec)
    all_shifts['startTime_s'] = all_shifts['startTime'].apply(str_to_sec) + 60 * (all_shifts['period'] - 1) * 20
    all_shifts['endTime_s'] = all_shifts['endTime'].apply(str_to_sec) + 60 * (all_shifts['period'] - 1) * 20
    
    all_shifts["type"] = "OTF"

    faceoffs = (pd.json_normalize(pbp_json["plays"])
                .query('typeDescKey=="faceoff"')
                .filter(['timeInPeriod','homeTeamDefendingSide', 'details.xCoord','details.zoneCode', 'period'])
                .assign(current_time = lambda x: x['timeInPeriod'].apply(str_to_sec) +20*60* (x['period']-1))
                .drop(columns=['timeInPeriod', 'period']))

    

    for _, shift in all_shifts.iterrows():

        time = shift["startTime_s"]
        if time in faceoffs["current_time"].values:
            matching_faceoffs = faceoffs.query("current_time == @time")
            zoneCode = matching_faceoffs["details.zoneCode"].values[0]
            homeTeamZone = matching_faceoffs["homeTeamDefendingSide"].values[0]
            xCoord = matching_faceoffs["details.xCoord"].values[0]



            if zoneCode == "N":
                all_shifts.at[_, "type"] = "NZF"
            elif (
                homeTeamZone == "left" and shift["is_home"] == 1 and xCoord < 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 1 and xCoord > 0
            ) or (
                homeTeamZone == "left" and shift["is_home"] == 0 and xCoord > 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 0 and xCoord < 0
            ):
                all_shifts.at[_, "type"] = "DZF"
            elif (
                homeTeamZone == "left" and shift["is_home"] == 1 and xCoord > 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 1 and xCoord < 0
            ) or (
                homeTeamZone == "left" and shift["is_home"] == 0 and xCoord < 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 0 and xCoord > 0
            ):
                all_shifts.at[_, "type"] = "OZF"
        else:
            all_shifts.at[_, "type"] = "OTF"

    all_shifts['date'] = pbp_json['gameDate']
    all_shifts['season'] = pbp_json['season']
    all_shifts['gameType'] = game_id


    return all_shifts


#Scrape game

### STILL HAVE TO CLEAN UP THE COLUMNS OF THE DATAFRAME ###
def scrape_game(game_id: int, pbp_json: Union[Dict, None] = None, game_rosters: Union[pd.DataFrame, None] = None, html_shifts: Union[pd.DataFrame, None] = None,
                full_pbp: bool = True) -> Dict:
    
    '''
    Scrape game from NHL API and return a dictionary of dataframes for each table.

    Parameters
    ----------
    game_id : int
        Game ID to scrape.
    pbp_json : Union[Dict, None], optional
        Play-by-play JSON for game. The default is None.
    game_rosters : Union[pd.DataFrame, None], optional
        Game rosters dataframe. The default is None.
    html_shifts : Union[pd.DataFrame, None], optional
        Shifts dataframe. The default is None.
    full_pbp : bool, optional
        Whether to return full play-by-play dataframe. The default is True.
    '''
    
    pbp_json = fetch_play_by_play_json(game_id) if pbp_json is None else pbp_json
    game_rosters = fetch_game_rosters(game_id) if game_rosters is None else game_rosters
    # html_shifts = fetch_html_shifts(game_id) if html_shifts is None else html_shifts

    html_shifts = fetch_html_shifts2(game_id) if html_shifts is None else html_shifts

    gameType = "preseason" if pbp_json.get("gameType", []) == 1 else ("regular-season" if pbp_json.get("gameType", []) == 2 else "playoffs")

    df = (pd.json_normalize(pbp_json.get("plays", []))
           .assign(game_id = game_id,
                      gameType = gameType,
                      season = pbp_json.get("season", []),
                      venue = pbp_json.get("venue", []).get("default", None),
                      startTimeUTC = pbp_json.get("startTimeUTC", []),
                      home_abbr = pbp_json.get("homeTeam", {}).get("abbrev", None),
                      home_name = pbp_json.get("homeTeam", []).get("name", {}).get("default", None),
                      home_logo = pbp_json.get("homeTeam", {}).get("logo", None),
                      away_abbr = pbp_json.get("awayTeam", {}).get("abbrev", None),
                      away_name = pbp_json.get("awayTeam", []).get("name", {}).get("default", None),
                      away_logo = pbp_json.get("awayTeam", {}).get("logo", None),
                      ))
    
    

    df = format_columns(df)
    df = elapsed_time(df)

    df = add_missing_columns(df)
    df = add_event_players_info(df, game_rosters)

    #Column names
    df.columns = [col.split('.')[-1] for col in df.columns]

    if full_pbp :
        df = process_pbp(df, html_shifts, game_rosters,True)
        df = process_pbp(df, html_shifts, game_rosters, False)
        df = strength(df)

        df.drop(columns=[ 'winningPlayerId', 'losingPlayerId',
       'hittingPlayerId', 'hitteePlayerId', 'shootingPlayerId',
       'goalieInNetId', 'playerId', 'blockingPlayerId', 'scoringPlayerId',
       'assist1PlayerId', 'assist2PlayerId', 'committedByPlayerId',
       'drawnByPlayerId', 'servedByPlayerId', 'situationCode', 'sortOrder','eventId', 'number',], inplace=True)

    else:
        df = df
        df.drop(columns=[ 'winningPlayerId', 'losingPlayerId',
       'hittingPlayerId', 'hitteePlayerId', 'shootingPlayerId',
       'goalieInNetId', 'playerId', 'blockingPlayerId', 'scoringPlayerId',
       'assist1PlayerId', 'assist2PlayerId', 'committedByPlayerId',
       'drawnByPlayerId', 'servedByPlayerId', 'situationCode', 'sortOrder','eventId', 'number',], inplace=True)
    
    return df


#Get the TOI per player per strength for a given game.
def get_strength_toi_per_team(game_id=2023020005, game_rosters: Union[pd.DataFrame, None] = None, html_shifts: Union[pd.DataFrame, None] = None):

    ''' 
    Get the TOI per strength for a given game.

    Parameters
    ----------
    game_id : int
        Game ID to scrape.
    game_rosters : Union[pd.DataFrame, None], optional
        Game rosters dataframe. The default is None.
    html_shifts : Union[pd.DataFrame, None], optional
        Shifts dataframe. The default is None.
    is_home : bool, optional
        Whether to get the home or away players. The default is True.
    '''

    html_shifts = fetch_html_shifts2(game_id) if html_shifts is None else html_shifts
    game_rosters = fetch_game_rosters(game_id) if game_rosters is None else game_rosters

    is_home = 1
    place = 'home' if is_home else 'away'
    not_place = 'away' if is_home else 'home'

    home_player_counts = get_player_count_per_second(game_id, html_shifts=html_shifts, is_home=1) #Gotta work on a fix with game_rosters
    away_player_counts = get_player_count_per_second(game_id, html_shifts=html_shifts, is_home=(0)) #Gotta work on a fix with game_rosters

    df = home_player_counts.merge(away_player_counts, on=["Second", "game_id"], how="left")

    df['is_home2'] = 1 if is_home else 0
    df['home_strength'] = df.apply(lambda row: f'{row[f"{place}Count"]}v{row[f"{not_place}Count"]}', axis=1)
    df['away_strength'] = df.apply(lambda row: f'{row[f"{not_place}Count"]}v{row[f"{place}Count"]}', axis=1)

    df = pd.concat([(df.home_strength
            .value_counts()
            .reset_index()
            .assign(is_home=1)
            .rename(columns={"home_strength" : "strength", "count" : "TOI"})),
            (df.away_strength
            .value_counts()
            .reset_index()
            .assign(is_home=0)
            .rename(columns={"away_strength" : "strength", "count" : "TOI"}))])
    
    pbp_json = fetch_play_by_play_json(game_id)

    df["abbrev"] = pbp_json['homeTeam']["abbrev"]
    df["name"] = pbp_json['homeTeam']["name"]

    df.loc[df['is_home']==0, 'abbrev'] = pbp_json['awayTeam']['abbrev']
    df.loc[df['is_home']==0, 'name'] = pbp_json['awayTeam']['name']

    df["game_id"] = game_id

    df = df.query("strength not in ['0v0']").reset_index(drop=True)

    
    return df

#TOI Manips
def get_player_count_per_second(game_id=2023020005, game_rosters: Union[pd.DataFrame, None] = None, html_shifts: Union[pd.DataFrame, None] = None, is_home=True):
    '''
    Get the number of players on the ice per second for a given game.

    Parameters
    ----------
    game_id : int
        Game ID to scrape.
    game_rosters : Union[pd.DataFrame, None], optional
        Game rosters dataframe. The default is None.
    html_shifts : Union[pd.DataFrame, None], optional
        Shifts dataframe. The default is None.
    is_home : bool, optional
        Whether to get the home or away players. The default is True.
    '''

    place = 'home' if is_home else 'away'

    html_shifts = fetch_html_shifts2(game_id) if html_shifts is None else html_shifts
    game_rosters = fetch_game_rosters(game_id) if game_rosters is None else game_rosters

    

    df = html_shifts.copy().query("is_home==@is_home")

    # Create a time-based range
    game_duration = df['endTime_s'].max()  # Assumes the endTime_s column represents the game duration
    time_range = range(game_duration)  # +1 to include the last second

    # Create a DataFrame with all seconds in the game
    time_df = pd.DataFrame({'Second': time_range})

    # Calculate player counts for each second
    def count_players_on_ice(second):
        on_ice = df[(second >= df['startTime_s']) & (second < df['endTime_s'])
                    & (df['positionCode'].isin(['C', 'D', 'L', 'R']))]['sweaterNumber'].nunique()  # Adjust position codes as needed
        return on_ice

    time_df[f'{place}Count'] = time_df['Second'].apply(count_players_on_ice)

    time_df["game_id"] = game_id

    # Print the resulting DataFrame
    return time_df

def get_player_ids_per_second(game_id=2023020005, game_rosters: Union[pd.DataFrame, None] = None, html_shifts: Union[pd.DataFrame, None] = None, is_home=True):
    
    '''
    Get the player IDs on the ice per second for a given game.

    Parameters
    ----------
    game_id : int
        Game ID to scrape.
    game_rosters : Union[pd.DataFrame, None], optional
        Game rosters dataframe. The default is None.
    html_shifts : Union[pd.DataFrame, None], optional   
        Shifts dataframe. The default is None.
    is_home : bool, optional
        Whether to get the home or away players. The default is True.

    '''
    html_shifts = fetch_html_shifts2(game_id) if html_shifts is None else html_shifts
    game_rosters = fetch_game_rosters(game_id) if game_rosters is None else game_rosters

    place = 'home' if is_home else 'away'

    df = html_shifts.copy().query("is_home==@is_home")
    
    # Create a time-based range
    game_duration = df['endTime_s'].max()  # Assumes the endTime_s column represents the game duration
    time_range = range(game_duration)  # +1 to include the last second

    # Create an empty list to store the sweater numbers per second
    playerId_per_second = []

    # Iterate through each second and collect sweater numbers
    for second in time_range:
        on_ice = df[(second >= df['startTime_s']) & (second < df['endTime_s'])
                    & (df['positionCode'].isin(['C', 'D', 'L', 'R']))] # Adjust position codes as needed
        playerId = list(set(on_ice['playerId'].tolist()))
        playerId_per_second.append(playerId)

    # Create a DataFrame with all seconds in the game
    time_df = pd.DataFrame({'Second': time_range})

    time_df[f'{place}Players'] = playerId_per_second

    time_df["game_id"] = game_id


    # Print the resulting list
    return time_df

def players_toi_per_strength(game_id=2023020005, game_rosters: Union[pd.DataFrame, None] = None, html_shifts: Union[pd.DataFrame, None] = None, is_home=True):
    '''
    Get the TOI per player per strength for a given game.

    Parameters
    ----------
    game_id : int
        Game ID to scrape.
    game_rosters : Union[pd.DataFrame, None], optional
        Game rosters dataframe. The default is None.
    html_shifts : Union[pd.DataFrame, None], optional
        Shifts dataframe. The default is None.
    is_home : bool, optional
        Whether to get the home or away players. The default is True.
    '''

    html_shifts = fetch_html_shifts2(game_id) if html_shifts is None else html_shifts
    game_rosters = fetch_game_rosters(game_id) if game_rosters is None else game_rosters
    
    place = 'home' if is_home else 'away'
    not_place = 'away' if is_home else 'home'

    

    df = get_player_ids_per_second(game_id, game_rosters=game_rosters, html_shifts=html_shifts, is_home=is_home)
    df = df.explode(f'{place}Players')

    home_player_counts = get_player_count_per_second(game_id, html_shifts=html_shifts, is_home=is_home) #Gotta work on a fix with game_rosters
    away_player_counts = get_player_count_per_second(game_id, html_shifts=html_shifts, is_home=(not is_home)) #Gotta work on a fix with game_rosters

    df = df.merge(home_player_counts.merge(away_player_counts, on=["Second", "game_id"], how="left"), on=["Second", "game_id"], how="left")

    df['is_home2'] = 1 if is_home else 0
    df['strength'] = df.apply(lambda row: f'{row[f"{place}Count"]}v{row[f"{not_place}Count"]}', axis=1)

    result = (df.groupby([f'{place}Players', 'strength'], as_index=False)
                .size()
                .rename(columns={f'{place}Players': 'playerId',
                                 'size': 'Seconds'}))
    
    result = result.merge(game_rosters.query("is_home==@is_home"), on="playerId", how="left")
    result['strength'] = result['strength'].astype(str).str.replace('.0', '')

    # df2 = get_player_count_per_second(game_id=game_id, game_rosters=game_rosters,html_shifts=html_shifts,is_home=True).merge( get_player_count_per_second(game_id=game_id,html_shifts=html_shifts,is_home=False),  on=["Second", "game_id"], how="left")


    return result

