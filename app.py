
from flask import Flask, jsonify, request, render_template, redirect, url_for
import numpy as np
import plotly.graph_objects as go
from itertools import groupby
import psycopg2
from psycopg2.extras import RealDictCursor
# Python file that contains variables with a username, password, port # and database name
from postgreslogin import un,pw,port,db_name

app = Flask(__name__)


# Database connection configuration
db_config = {
    'host': 'localhost',
    'port': port,
    'database': db_name,
    'user': un,
    'password': pw
}

# Endpoint for getting all game prices
@app.route('/game-prices')
def get_game_prices():
    # Database connection information
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT * FROM avg_game_prices')
    results = cur.fetchall()
    cur.close()
    return jsonify(results)

# Endpoint for getting all console names
@app.route('/consoles')
def get_consoles():
    # Database connection information
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT console FROM consoles')
    results = cur.fetchall()
    cur.close()
    return jsonify(results)

# Endpoint for getting average game prices by console
@app.route('/avg-game-prices-by-console')
def get_avg_game_prices_by_console():
    # Database connection information
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute('SELECT console, AVG(loose_val) AS avg_loose_val, AVG(complete_val) AS avg_complete_val, AVG(new_val) AS avg_new_val FROM avg_game_prices JOIN consoles ON avg_game_prices.console_id = consoles.console_id GROUP BY console')
    results = cur.fetchall()
    cur.close()
    return jsonify(results)

@app.route('/games_by_console', methods=['GET','POST'])
def games_by_console():
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    
    # Retrieve list of consoles for the dropdown menu
    cur.execute("SELECT console_id, console FROM consoles ORDER BY console")
    console_list = cur.fetchall()
    
    # Handle form submission
    if request.method == 'POST':
        # Get selected console ID from form
        selected_console = request.form['console']
        
        # Query database for games matching selected console ID
        cur.execute("""
            SELECT game_id, loose_val, complete_val, new_val, date_scraped, url
            FROM avg_game_prices
            WHERE console_id = %s
            ORDER BY game_id
        """, (selected_console,))
        
        # Get list of games matching selected console ID
        games = cur.fetchall()
        
        return render_template('games_by_console.html', console_list=console_list, games=games)
    
    return render_template('games_by_console.html', console_list=console_list)

@app.route('/', methods=['GET', 'POST'])
def index():
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute("SELECT * FROM consoles;")
    consoles = cur.fetchall()

    if request.method == 'POST':
        console_id = request.form.get('console_id')
        if console_id:
            # Redirect to the page that contains the game dropdown
            return redirect(url_for('game_dropdown', console_id=console_id))

    return render_template('console_dropdown.html', consoles=consoles)

@app.route('/games/<console_id>', methods=['GET', 'POST'])
def game_dropdown(console_id):
    conn = psycopg2.connect(**db_config)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM avg_game_prices WHERE console_id = %s;", (console_id,))
        games = cur.fetchall()

    if request.method == 'POST':
        game_id = request.form.get('game_id')
        if game_id:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM loose_game_prices WHERE game_id = %s;", (game_id,))
                loose_prices = cur.fetchall()
                # get game_url by game_id
                cur.execute("SELECT game_url FROM avg_game_prices WHERE game_id = %s;", (game_id,))
                game_url = cur.fetchone()[0]

                # create data for the graph
                x = [price[1] for price in loose_prices]
                y = [float(price[2].replace('$', '').replace(',', '')) for price in loose_prices]
                # Group y values by their corresponding x values
                groups = groupby(zip(x, y), key=lambda pair: pair[0])

                # Calculate the average of each group to get a single y value for each x value
                x_avg = []
                y_avg = []
                for key , group in groups:
                    group_y = [pair[1] for pair in group]
                    x_avg.append(key)
                    y_avg.append(sum(group_y) / len(group_y))

                # create the line graph
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=x_avg, y=y_avg, mode='lines', name='Average'))
                fig.add_trace(go.Scatter(y=y, x=x, mode='markers', name='Scatter'))


                # set the title of the graph to the game_url
                game_url = game_url.replace('-', ' ')
                game_url = game_url.title()
                fig.update_layout(title=game_url, xaxis=dict(title='Date'), yaxis=dict(title='Price sold ($)'))
                fig.update_yaxes(range=[0, max(y) + 0.05*max(y)])

                # convert the graph to HTML and add it to the div in prices.html
                loose_graph_html = fig.to_html(full_html=False)

                cur.execute("SELECT * FROM new_game_prices WHERE game_id = %s;", (game_id,))
                new_prices = cur.fetchall()
                # get game_url by game_id
                cur.execute("SELECT game_url FROM avg_game_prices WHERE game_id = %s;", (game_id,))
                game_url = cur.fetchone()[0]

                # create data for the graph
                x = [price[1] for price in new_prices]
                y = [float(price[2].replace('$', '').replace(',', '')) for price in new_prices]
                # Group y values by their corresponding x values
                groups = groupby(zip(x, y), key=lambda pair: pair[0])

                # Calculate the average of each group to get a single y value for each x value
                x_avg = []
                y_avg = []
                for key , group in groups:
                    group_y = [pair[1] for pair in group]
                    x_avg.append(key)
                    y_avg.append(sum(group_y) / len(group_y))

                # create the line graph
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=x_avg, y=y_avg, mode='lines', name='Average'))
                fig.add_trace(go.Scatter(y=y, x=x, mode='markers', name='Scatter'))

                # set the title of the graph to the game_url
                game_url = game_url.replace('-', ' ')
                game_url = game_url.title()
                fig.update_layout(title=game_url, xaxis=dict(title='Date'), yaxis=dict(title='Price sold ($)'))
                fig.update_yaxes(range=[0, max(y) + 0.05*max(y)])

                # convert the graph to HTML and add it to the div in prices.html
                new_graph_html = fig.to_html(full_html=False)

                cur.execute("SELECT * FROM cib_game_prices WHERE game_id = %s;", (game_id,))
                cib_prices = cur.fetchall()
                # get game_url by game_id
                cur.execute("SELECT game_url FROM avg_game_prices WHERE game_id = %s;", (game_id,))
                game_url = cur.fetchone()[0]

                # create data for the graph
                x = [price[1] for price in cib_prices]
                y = [float(price[2].replace('$', '').replace(',', '')) for price in cib_prices]
                # Group y values by their corresponding x values
                groups = groupby(zip(x, y), key=lambda pair: pair[0])

                # Calculate the average of each group to get a single y value for each x value
                x_avg = []
                y_avg = []
                for key , group in groups:
                    group_y = [pair[1] for pair in group]
                    x_avg.append(key)
                    y_avg.append(sum(group_y) / len(group_y))

                # create the line graph
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=x_avg, y=y_avg, mode='lines', name='Average'))
                fig.add_trace(go.Scatter(y=y, x=x, mode='markers', name='Scatter'))

                # set the title of the graph to the game_url
                game_url = game_url.replace('-', ' ')
                game_url = game_url.title()
                fig.update_layout(title=game_url, xaxis=dict(title='Date'), yaxis=dict(title='Price sold ($)'))
                fig.update_yaxes(range=[0, max(y) + 0.05*max(y)])

                # convert the graph to HTML and add it to the div in prices.html
                cib_graph_html = fig.to_html(full_html=False)

            return render_template('prices.html', loose_prices=loose_prices, new_prices=new_prices, cib_prices=cib_prices, loose_graph=loose_graph_html, new_graph=new_graph_html, cib_graph=cib_graph_html)

    return render_template('game_dropdown.html', games=games)


if __name__ == '__main__':
    app.run(debug=True)

 

