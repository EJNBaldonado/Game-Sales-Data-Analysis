import pandas as pd
from pandas import Series, DataFrame
import numpy as np
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter, Retry
import time
import psycopg2
# Python file that contains variables with a username, password, port # and database name
from postgreslogin import un,pw,port,db_name


def read_game_prices(file_path):
    '''Returns a dataframe from a filepath to a csv
    
    Args:
        string: file path to a csv

    Returns:
        (dataframe) Dataframe object containing the contents of the csv
    '''
    gs_df = pd.read_csv(file_path)
    gs_df = pd.DataFrame(gs_df)
    return gs_df

def clean_game_prices(gs_df):
    '''Returns a cleaned dataframe from the game sales dataframe
    
    Args:
        Dataframe: original dataframe

    Returns:
        (dataframe) Cleaned Dataframe object with url values and a game id
    '''
    clean_df = gs_df.copy()
    # Drop values with no average values
    clean_df = clean_df.dropna(subset=['loose_val', 'complete_val', 'new_val'])

    # Data engineer url column based on Game name
    clean_df['game_url'] = clean_df['game'].str.lower()
    clean_df['game_url'] = clean_df['game_url'].str.replace(' ', '-')
    clean_df['game_url'] = clean_df['game_url'].str.replace("[:\[\].#?/,]",'',regex=True).str.replace('amp;','')
    clean_df['game_url'] = clean_df['game_url'].str.replace('--', '-')
    console_game_list = list(zip(clean_df['console'], clean_df['game_url']))
    base_pc_url = 'https://www.pricecharting.com/game/'
    url_list = []
    for tpl in console_game_list:
        url = base_pc_url + f'{tpl[0]}' + '/' + f'{tpl[1]}'
        url_list.append(url)
    clean_df['url'] = url_list
    clean_df['game_id'] = range(len(clean_df))
    clean_df['game_id'] += 1

    # Special Cases
    clean_df.loc[clean_df['game_id'] == 3246, 'url'] = 'https://www.pricecharting.com/game/nintendo-64/ique-player'
    clean_df.loc[clean_df['game_id'] == 3913, 'url'] = 'https://www.pricecharting.com/game/gameboy-advance/ique-gameboy-advance'
    clean_df.loc[clean_df['game_id'] == 6769, 'url'] = 'https://www.pricecharting.com/game/wii/the-$1,000,000-pyramid'
    clean_df.loc[clean_df['game_id'] == 6932, 'url'] = 'https://www.pricecharting.com/game/nintendo-ds/black-reshiram-&-zekrom-edition-nintendo-dsi'
    return clean_df
    

def send_request(url,
    n_retries=4,
    backoff_factor=0.9,
    status_codes=[504, 503, 502, 500, 429]):
    '''Fixes issues with gamescraper sending requests to website to scrape
    
    Args:
        Only necessary argument is url, other arguments are better left default

    Returns:
        (response) response to website request to to retrieve data
    '''
    sess = requests.Session()
    retries = Retry(connect=n_retries, backoff_factor=backoff_factor,
     status_forcelist=status_codes)
    sess.mount("https://", HTTPAdapter(max_retries=retries))
    sess.mount("http://", HTTPAdapter(max_retries=retries))
    response = sess.get(url)
    return response

def indivgamescraper(url, game_id):
    '''Scrapes date sold and price sold values of the item in url
    
    Args:
        url: link of the item you're wishing to scrape

    Returns:
        (3 dataframes) cleaned dataframes of recently sold loose, cib, and new listings
    '''

    # Generate different urls for used, complete-in-box and sealed sales
    looseurl = url + '#completed-auctions-used'
    ciburl = url + '#completed-auctions-cib'
    newurl = url + '#completed-auctions-new'

    # Send request to website to retrieve data
    lresult = send_request(looseurl)
    cresult = send_request(ciburl)
    nresult = send_request(newurl)

    # Get content from website
    lc = lresult.content
    cc = cresult.content
    nc = nresult.content

    # Scrape websites HTML using BeautifulSoup to find appropriate tables
    lsoup = BeautifulSoup(lc, features='html.parser')
    csoup = BeautifulSoup(cc, features='html.parser')
    nsoup = BeautifulSoup(nc, features='html.parser')

    ldivid = lsoup.find('div', {'class': 'tab-frame'})
    cdivid = csoup.find('div', {'class': 'tab-frame'})
    ndivid = nsoup.find('div', {'class': 'tab-frame'})

    lsummary = ldivid.find_all('div', {'class':'completed-auctions-used'})
    csummary = cdivid.find_all('div', {'class':'completed-auctions-cib'})
    nsummary = ndivid.find_all('div', {'class':'completed-auctions-new'})

    # Create the dataframe from the scraped html table
    loose_df = html_cleaning(lsummary, game_id)
    cib_df = html_cleaning(csummary, game_id)
    new_df = html_cleaning(nsummary, game_id)
    print("Just created df's for", game_id)

    return loose_df, cib_df, new_df

def html_cleaning(summary, game_id):
    '''Uses a summary from a BeautifulSoup object to parse information into a DataFrame
    
    Args: 
        summary: Uses the created summary from the html division

    Returns: 
        (dataframe) Cleaned dataframe
    '''

    tables = summary[0].find_all('table')

    data=[]

    # If there is no table found, create an 'empty row'
    if len(tables) == 0:
        game_sales_df = DataFrame(columns=['date','price_sold','game_id'])
        new_row = DataFrame({'date': None, 'price_sold': 0, 'game_id': game_id}, index=[0])
        game_sales_df = pd.concat([game_sales_df, new_row])
        return game_sales_df
    
    # Otherwise, find the rows and collect the data
    rows = tables[0].findAll('tr')

    for tr in rows:
        cols = tr.findAll('td')
        for td in cols:
            texts = td.findAll(text=True)
            for text in texts:
                data.append(text)

    # Clean the data scraped
    cleanerdata = []    
    for item in data:
        if '\n' not in item:
            cleanerdata.append(item)
    cleandata = []
    for item in cleanerdata:
        if 'Report It' not in item:
            cleandata.append(item)

    # Create the dataframe
    index = 0 
    date = []
    price = []
    for item in cleandata:
        if index%2 == 0:
            date.append(item)
        else:
            price.append(item.replace('$',''))
        index +=1

    date = Series(date)
    price = Series(price)

    game_sales_df = pd.concat([date , price], axis=1)
    game_sales_df.columns = ['date','price_sold']
    game_sales_df['game_id'] = game_id

    return game_sales_df

def create_console_df(gs_df):
    '''Creates a separate df containing the different consoles and gives them a console id
    
    Args: 
        gs_df: Game sales dataframe

    Returns: 
        (dataframe) Dataframe with console and console id columns
    '''
    console_df = pd.DataFrame({'console': gs_df['console'].unique()})
    console_df['console_id'] = pd.factorize(console_df['console'])[0]
    return console_df

def add_console_id(gs_df, console_df):
    '''Adds console_id column to gs_df

    Args:
        gs_df: Game sales dataframe
        console_df: Console dataframe
    
    Returns:
        (dataframe) Mapped gs_df games a respective console id column
    '''
    console_map = dict(zip(console_df['console'], console_df['console_id']))
    gs_df['console_id'] = gs_df['console'].map(console_map)
    return gs_df

def create_connection(un, pw, port, db_name):
    '''Creates a connection to a postgres database
    
    Args:
        log-in info for database
    
    Returns:
        connection and a cursor
    '''
    try:
        conn = psycopg2.connect(dbname=db_name, user=un, password=pw, port=port, host='localhost')
        cursor = conn.cursor()
        cursor.execute('SELECT version();')
        result = cursor.fetchone()
        print("The version of PostgreSQL is:", result)
    except psycopg2.Error as e:
        print('Fail to execute due to the error:', e)
    return conn, cursor

def create_console_table(cursor):
    try:
        query = '''
            DROP TABLE IF EXISTS consoles CASCADE;
            CREATE TABLE consoles
            (console_id integer PRIMARY KEY,
            console varchar
            );
        '''
        cursor.execute(query)
        print('The consoles df has been created successfully')
    except psycopg2.Error as e:
        print('Fail to execute due to the error:', e)


def insert_console_values(cursor, console_df):
    try:
        values = zip(console_df['console_id'], console_df['console'])
        query = '''
                    INSERT INTO
                    consoles (console_id, console)
                    VALUES (%s, %s)
                    ;
                '''
        cursor.executemany(query, values)
        print('Inserted records into consoles')  
    except psycopg2.Error as e:
        print('Fail to execute due to the error:', e)

def create_avg_game_prices_table(cursor):
    try:
        query = '''
                    DROP TABLE IF EXISTS avg_game_prices CASCADE;
                    CREATE TABLE avg_game_prices
                    (game_id integer PRIMARY KEY,
                    console_id integer,
                    loose_val money,
                    complete_val money,
                    new_val money,
                    date_scraped varchar,
                    game_url varchar,
                    url varchar,
                    FOREIGN KEY (console_id) REFERENCES consoles (console_id)
                    );
                '''
        
        cursor.execute(query)

        print('The avg_game_prices table has been created successfully')
    except psycopg2.Error as e:
        print('Fail to execute due to the error:', e)

def insert_avg_game_prices_values(cursor, gs_df):
    try:
        values = zip(gs_df['game_id'], gs_df["console_id"], gs_df["loose_val"], gs_df["complete_val"], gs_df["new_val"], gs_df["date(D/M/Y)"], gs_df["game_url"], gs_df["url"])

        query = '''
                INSERT INTO
                avg_game_prices (game_id, console_id, loose_val, complete_val, new_val, date_scraped, game_url, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
                ;
                '''
        cursor.executemany(query, values)
        print('Inserted records into avg_game_prices')
    except psycopg2.Error as e:
        print('Fail to execute due to the error:', e)

def create_recent_sales_tables(cursor):
    try:
        query = '''
                DROP TABLE IF EXISTS loose_game_prices;
                CREATE TABLE loose_game_prices
                (game_id integer,
                date_sold varchar,
                price_sold money,
                FOREIGN KEY (game_id) REFERENCES avg_game_prices (game_id)
                );                
                '''
        cursor.execute(query)
        print('The loose_game_prices table has been created successfully')

        query = '''
                DROP TABLE IF EXISTS cib_game_prices;
                CREATE TABLE cib_game_prices
                (game_id integer,
                date_sold varchar,
                price_sold money,
                FOREIGN KEY (game_id) REFERENCES avg_game_prices (game_id)
                );                
                '''
        cursor.execute(query)
        print('The cib_game_prices table has been created successfully')

        query = '''
                DROP TABLE IF EXISTS new_game_prices;
                CREATE TABLE new_game_prices
                (game_id integer,
                date_sold varchar,
                price_sold money,
                FOREIGN KEY (game_id) REFERENCES avg_game_prices (game_id)
                );                
                '''
        cursor.execute(query)
        print('The new_game_prices table has been created successfully') 
    except psycopg2.Error as e:
        print('Fail to execute due to the error:', e) 

def update_recent_sales_tables(cursor, console_df):
    loose_df = pd.DataFrame(columns=['date', 'price_sold', 'game_id'])
    cib_df = pd.DataFrame(columns=['date', 'price_sold', 'game_id'])
    new_df = pd.DataFrame(columns=['date', 'price_sold', 'game_id'])
    for i in range(len(console_df)):
        time.sleep(1)
        url = console_df.iloc[i]['url']
        id = console_df.iloc[i]['game_id']
        g_name = console_df.iloc[i]['game']
        print('Trying to add', g_name)
        l_df, c_df, n_df = indivgamescraper(url, id)
        loose_df = pd.concat([loose_df, l_df])
        cib_df = pd.concat([cib_df, c_df])
        new_df = pd.concat([new_df, n_df])
    values = zip(loose_df['date'], loose_df['price_sold'], loose_df['game_id'])
    query = '''
            INSERT INTO loose_game_prices (date_sold, price_sold, game_id)
            VALUES (%s, %s, %s)
            ;
            '''
    cursor.executemany(query, values)
    print('Inserted values into loose_game_prices')

    values = zip(cib_df['date'], cib_df['price_sold'], cib_df['game_id'])
    query = '''
            INSERT INTO cib_game_prices (date_sold, price_sold, game_id)
            VALUES (%s, %s, %s)
            ;
            '''
    cursor.executemany(query, values)
    print('Inserted values into cib_game_prices')

    values = zip(new_df['date'], new_df['price_sold'], new_df['game_id'])
    query = '''
            INSERT INTO new_game_prices (date_sold, price_sold, game_id)
            VALUES (%s, %s, %s)
            ;
            '''
    cursor.executemany(query, values)
    print('Inserted values into new_game_prices')



def main():
    gs_df = read_game_prices('game_prices.csv')
    gs_df = clean_game_prices(gs_df)
    console_df = create_console_df(gs_df)
    gs_df = add_console_id(gs_df, console_df)
    conn, cursor =create_connection(un, pw, port, db_name)  
    create_console_table(cursor)
    insert_console_values(cursor, console_df)
    create_avg_game_prices_table(cursor)
    insert_avg_game_prices_values(cursor, gs_df)
    create_recent_sales_tables(cursor)

    # Group the data by console_id and create a dictionary of dataframes
    console_dataframes = {}
    for console_id, console_df in gs_df.groupby('console_id'):
        console_dataframes[console_id] = console_df
    
    # Update the dataframe for each console
    for console_id, console_df in console_dataframes.items():
        update_recent_sales_tables(cursor, console_df)


    # Make the changes to the database persistent
    conn.commit()
    # Close the cursor
    cursor.close()

if __name__ == "__main__":
    print('Starting Pricecharting API process\n')
    main()
    print('Finished Pricecharting API process')