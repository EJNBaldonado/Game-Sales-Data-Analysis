from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import time
import psycopg2

def send_request(url,
    n_retries=4,
    backoff_factor=0.9,
    status_codes=[504, 503, 502, 500, 429]):
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
    looseurl = url + '#completed-auctions-used'
    ciburl = url + '#completed-auctions-cib'
    newurl = url + '#completed-auctions-new'

    # timeout didn't work as well
    # verify=False got stuck at 249 
    # Another option is to use try except block for connectionerror
    '''
    lresult = requests.get(looseurl, verify=False)
    curl = requests.get(ciburl, verify=False)
    nurl = requests.get(newurl, verify=False)
    '''
    lresult = send_request(looseurl)
    curl = send_request(ciburl)
    nurl = send_request(newurl)

    lc = lresult.content
    cc = curl.content
    nc = nurl.content

    lsoup = BeautifulSoup(lc, features='html.parser')
    csoup = BeautifulSoup(cc, features='html.parser')
    nsoup = BeautifulSoup(nc, features='html.parser')

    ldivid = lsoup.find('div', {'class': 'tab-frame'})
    cdivid = csoup.find('div', {'class': 'tab-frame'})
    ndivid = nsoup.find('div', {'class': 'tab-frame'})

    lsummary = ldivid.find_all('div', {'class':'completed-auctions-used'})
    csummary = cdivid.find_all('div', {'class':'completed-auctions-cib'})
    nsummary = ndivid.find_all('div', {'class':'completed-auctions-new'})

    loose_df = html_cleaning(lsummary, game_id)
    cib_df = html_cleaning(csummary, game_id)
    new_df = html_cleaning(nsummary, game_id)
    print('Just created df for', game_id)

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

    # If
    if len(tables) == 0:
        game_sales_df = DataFrame(columns=['date','price_sold','game_id'])
        new_row = DataFrame({'date': None, 'price_sold': 0, 'game_id': game_id}, index=[0])
        game_sales_df = pd.concat([game_sales_df, new_row])
        return game_sales_df
    

    rows = tables[0].findAll('tr')


    for tr in rows:
        cols = tr.findAll('td')
        for td in cols:
            texts = td.findAll(text=True)
            for text in texts:
                # print(text)
                data.append(text)


    cleanerdata = []

    for item in data:
        if '\n' not in item:
            cleanerdata.append(item)

    cleandata = []

    for item in cleanerdata:
        if 'Report It' not in item:
            cleandata.append(item)

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


            
def main():

    # Read in scraped average values
    gs_df = pd.read_csv('game_prices.csv')
    gs_df = DataFrame(gs_df)

    
    # Data cleaning
    gs_df = gs_df.dropna(subset=['loose_val','complete_val', 'new_val'])

    # Create column for the game title portion of the 'url' (created from game title)
    gs_df['game_url'] = gs_df['game'].str.lower()
    gs_df['game_url'] = gs_df['game_url'].str.replace(' ', '-')
    gs_df['game_url'] = gs_df['game_url'].str.replace("[:\[\].#?/,]",'',regex=True).str.replace('amp;','')
    gs_df['game_url'] = gs_df['game_url'].str.replace('--', '-')

    # Combine with the base pricecharting.com base url for the completed url
    console_game_list = list(zip(gs_df['console'], gs_df['game_url']))
    base_pc_url = 'https://www.pricecharting.com/game/'
    url_list = []
    for tpl in console_game_list:
        url = base_pc_url + f'{tpl[0]}' + '/' + f'{tpl[1]}'
        url_list.append(url)
    
    # Create 'url' and a 'game_id' column
    gs_df['url'] = url_list
    gs_df['game_id'] = range(len(gs_df))
    gs_df['game_id'] += 1
    # Special case with iQue player for Nintendo64, and for Gameboy
    gs_df.loc[gs_df['game_id'] == 3246, 'url'] = 'https://www.pricecharting.com/game/nintendo-64/ique-player'
    gs_df.loc[gs_df['game_id'] == 3913, 'url'] = 'https://www.pricecharting.com/game/gameboy-advance/ique-gameboy-advance'
    # Special case for $100000 pyramid for wii
    gs_df.loc[gs_df['game_id'] == 6769, 'url'] = 'https://www.pricecharting.com/game/wii/the-$1,000,000-pyramid'
    # Special case for pokemon black nintendo dsi system
    gs_df.loc[gs_df['game_id'] == 6932, 'url'] = 'https://www.pricecharting.com/game/nintendo-ds/black-reshiram-&-zekrom-edition-nintendo-dsi'

    # Create a new DataFrame with unique values of Console
    console_df = pd.DataFrame({'console': gs_df['console'].unique()})
    console_df['console_id'] = pd.factorize(console_df['console'])[0]

    # Create a dictionary to map original values to id's
    console_map = dict(zip(console_df['console'], console_df['console_id']))

    # Create a new column in gs_df with mapped id's
    gs_df['console_id'] = gs_df['console'].map(console_map)


    # Connect to postgresSql
    un = "postgres"
    pw = 'Sagepokenexus1!'
    port = 5432
    db_name = 'Game Sales Prices'

    try:
        # set the connection
        conn = psycopg2.connect(dbname=db_name, user=un, password=pw, port=port, host='localhost')
        # Initialize the cursor
        cursor = conn.cursor()
        # cursor execute to query
        cursor.execute('SELECT version();')
        result = cursor.fetchone()
        print("The version of PostgreSQL is:", result)

# Create the table that holds console name information
        query = '''
                    DROP TABLE IF EXISTS consoles CASCADE;
                    CREATE TABLE consoles
                    (console_id integer PRIMARY KEY,
                    console varchar
                    );
                '''
        cursor.execute(query)

        print('The consoles df has been created successfully')

        values = zip(console_df['console_id'], console_df['console'])
        query = '''
                    INSERT INTO
                    consoles (console_id, console)
                    VALUES (%s, %s)
                    ;
                '''
        cursor.executemany(query, values)
        print('Inserted records into consoles')

# Creation of the table for Avg game prices

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

        values = zip(gs_df['game_id'], gs_df["console_id"], gs_df["loose_val"], gs_df["complete_val"], gs_df["new_val"], gs_df["date(D/M/Y)"], gs_df["game_url"], gs_df["url"])

        query = '''
                INSERT INTO
                avg_game_prices (game_id, console_id, loose_val, complete_val, new_val, date_scraped, game_url, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
                ;
                '''
        cursor.executemany(query, values)
        print('Inserted records into avg_game_prices')


# Creation of the tables for recent sales of each of the games

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

        loose_df = pd.DataFrame(columns=['date', 'price_sold', 'game_id'])
        cib_df = pd.DataFrame(columns=['date', 'price_sold', 'game_id'])
        new_df = pd.DataFrame(columns=['date', 'price_sold', 'game_id'])

        # game_id==6932 Pokemon black nintendo dsi system replaced by 'black-reshiram-&-zekrom-edition-nintendo-dsi'
        # Error for another iQue at 3913
        #gs_new = gs_df.query("game_id==3913")
        #print(gs_new["url"].iloc[0])
        '''
        gs_new = gs_df.query("game_id==613")
        #print(gs_new["url"].iloc[0])
        url = gs_df['url'][230]
        print(url)
        id = gs_df['game_id'][230]
        g_name = gs_df['game'][230]
        print('Trying to add', g_name)
        l_df, c_df, n_df = indivgamescraper(url, id)
        print('Inserting ', id, ' into large df')
        loose_df = pd.concat([loose_df, l_df])
        cib_df = pd.concat([cib_df, c_df])
        new_df = pd.concat([new_df, n_df])
        '''
        #error at 5377 skyward sword SOLVED by adding one second delay
        #error at 5608 sakura wars: so long, my love
        # once you know it works up to a certain point restart the process from that point , i.e. 3912
        # 6769 1000000 pyramid error
        #gs_new = gs_df.query("game_id==6769")
        #print(gs_new["url"].iloc[0])    

        # New error ConnectionError: HTTPSConnectionPool(host='www.pricecharting.com', port=443): Max retries exceeded with url:
        # Added timeout

        
        for i in range(len(gs_df)):
            time.sleep(1)
            #if i == 41 or i == 230:
            #    i += 1
            # replaced [i] for .iloc[i]
            #url = gs_df['url'][i]
            #id = gs_df['game_id'][i]
            #g_name = gs_df['game'][i]
            url = gs_df.iloc[i]['url']
            id = gs_df.iloc[i]['game_id']
            g_name = gs_df.iloc[i]['game']
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

        #make the changes to the database persistent
        conn.commit()



    except psycopg2.Error as e:
        print('Fail to execute due to the error:', e)

    cursor.close()
if __name__ == "__main__":
    print('Starting Pricecharting API process\n')
    main()
    print('Finished Pricecharting API process')

