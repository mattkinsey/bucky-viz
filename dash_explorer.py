from dash import Dash
from dash import dcc
from dash import html
from dash import Input, Output

#from dash import Dash, dcc, html, Input, Output

import pandas as pd
import zipfile
import plotly.express as px
import plotly.graph_objects as go
import us

import os
path = os.getcwd() + '/util/'
print(path)
import sys
sys.path.append(path)

from analytics_util import *
from color_setup import *


def prepare_data(b_path,r_path, m_str):

    # USE LOCAL PATH to cloned data files
    bucky_data_path = b_path  
    reichlab_data_path = r_path  
    
    show_paths()

    # Find locally loaded data folders

    monday_str = m_str #'2022-08-15'  # RUN DATE: override to load known data
    
    b_output_files_list = get_most_recent_bucky_output_folders(bucky_data_path, monday_str)
    
    if len(b_output_files_list) == 0:
        print('Need data path.')
    
    # GET ALL CURRENT BUCKY OUTPUT
    
    sys.path.append(bucky_data_path)

    df_b_out = pd.DataFrame({})
    df_b_o = pd.DataFrame({})
    df_b_o_us = pd.DataFrame({})
    df_b_o_cat = pd.DataFrame({})

    for i in b_output_files_list:
        #print(i)
        df_b_o_us = get_bucky_output_df(bucky_data_path, i)
        df_b_o = get_bucky_output_1_df(bucky_data_path, i)
        df_b_o_cat = pd.concat([df_b_o_us, df_b_o], ignore_index=True)
        df_b_out = pd.concat([df_b_out, df_b_o_cat], ignore_index=True)
        #print(df_b_out.to_string())

    df_b_out['dt'] = pd.to_datetime(df_b_out['date'], format='%Y-%m-%d')
    df_b_out = df_b_out[df_b_out['quantile'].isin([-1,.01,.2,.25,.45,.55,.75,.8,.5,.99])]

    df_b_out['adm1'] = df_b_out['adm1'].astype(float)
    df_b_out.drop(['adm0'],axis=1, inplace=True)

    
    #b_models.append('Historic')
    #b_models


    # CREATE STATE TEMPLATE CODES

    #print(us.states.STATES)
    #print(us.states.STATES_AND_TERRITORIES)
    l = us.states.STATES_AND_TERRITORIES

    s_name=[]
    s_fips=[]
    s_abbr=[]
    for i in l:
        s_name.append(str(i))
        s_fips.append(i.fips)
        s_abbr.append(i.abbr)
        #print(i, i.abbr, i.fips)

    s_name.insert(0,'United States')
    s_fips.insert(0,'-1')
    s_abbr.insert(0,'US')

    df_us = pd.DataFrame({'State':s_name,'adm1':s_fips,'abbr':s_abbr})
    df_us['adm1'] = df_us['adm1'].astype(float)
    df_us.sample(3)



    # GET REICHLAB TRUTH  with State and National Data

    def prior_30_date(given_date):  # TAKES datetime.date(YYYY, MM, DD)
        return given_date - timedelta(30)

    def future_30_date(given_date):  # TAKES datetime.date(YYYY, MM, DD)
        return given_date + timedelta(30)

    # load Data HISTORIC DATA

    sys.path.append(reichlab_data_path)


    # load incident truth
    df_ic = pd.read_csv(reichlab_data_path+'data-truth/truth-Incident Cases.csv', low_memory=False)
    df_id = pd.read_csv(reichlab_data_path+'data-truth/truth-Incident Deaths.csv', low_memory=False)
    df_ih = pd.read_csv(reichlab_data_path+'data-truth/truth-Incident Hospitalizations.csv', low_memory=False)

    last_date = df_ic['date'].max() 
    # I am assuming that all three of these files have the same final date. !!!!

    in_string = str(last_date)
    f = "%Y-%m-%d"  # YEAR - MONTH - DAY    YYYY-MM-DD
    in_object = datetime.strptime(in_string, f)

    prior_30 = prior_30_date(in_object)
    prior_30_str = str(prior_30.date())
    future_30 = future_30_date(in_object)
    future_30_str = str(future_30.date())

    l = df_ic['location'].unique()
    ll = []
    for i in l:
        if len(i)<5:
            ll.append(i)

    df_us_c = df_ic.loc[df_ic['location'].isin(ll)]
    df_us_d = df_id.loc[df_id['location'].isin(ll)]
    df_us_h = df_ih.loc[df_ih['location'].isin(ll)]




    # 7 day rolling average to smooth weekends
    df_us_ic = df_us_c.copy()
    df_us_ic['value'] = df_us_ic['value'].rolling(7).mean()
    df_us_ic.dropna(how='any', axis=0, inplace=True)
    df_us_id = df_us_d.copy()
    df_us_id['value'] = df_us_id['value'].rolling(7).mean()
    df_us_id.dropna(how='any', axis=0, inplace=True)
    df_us_ih = df_us_h.copy()
    df_us_ih['value'] = df_us_ih['value'].rolling(7).mean()
    df_us_ih.dropna(how='any', axis=0, inplace=True)

    # 30 days hist crop
    df_us_ic_crop = df_us_ic[(df_us_ic['date'] > prior_30_str) & (df_us_ic['date'] <= future_30_str)]
    df_us_id_crop = df_us_id[(df_us_id['date'] > prior_30_str) & (df_us_id['date'] <= future_30_str)]
    df_us_ih_crop = df_us_ih[(df_us_ih['date'] > prior_30_str) & (df_us_ih['date'] <= future_30_str)]

    dups = ['date','location','location_name']
    df_us_ic_crop.columns = dups + ['daily_reported_cases']
    df_us_id_crop.columns = dups + ['daily_deaths']
    df_us_ih_crop.columns = dups + ['daily_hospitalizations']

    df_us_all = pd.merge(df_us_ih_crop, df_us_ic_crop, how='inner', left_on=dups, right_on=dups)
    df_us_all = pd.merge(df_us_all, df_us_id_crop, how='inner', left_on=dups, right_on=dups)
    df_us_all.sort_values(by='date', ascending=False,inplace=True)
    df_us_all.reset_index(inplace=True)
    df_us_all['model'] = 'Historic'#+monday_str
    df_us_all['quantile'] = -1
    df_us_all['adm1'] = df_us_all['location']
    df_us_all['State'] = df_us_all['location_name']
    df_us_all = df_us_all.drop(['location','location_name','index'],axis=1)

    df_us_all['dt'] = pd.to_datetime(df_us_all['date'], format='%Y-%m-%d')
    df_us_all['adm1'].replace('US','-1', inplace=True)  # US --> -1
    df_us_all['adm1'] = df_us_all['adm1'].astype(float)
    df_us_all = df_us_all.merge(df_us, how='inner', on=['adm1'])
    df_us_all.drop(['State_y'], axis='columns', inplace=True)
    df_us_all.rename(columns = {'State_x':'State'}, inplace = True)
    
    
    df_b_out2 = df_b_out.merge(df_us, left_on=['adm1'], right_on=['adm1'])

    # ADD ReichLab HISTORIC TRUTH
    df_b_out3 = pd.concat([df_us_all, df_b_out2], axis=0, ignore_index=True)
    #print(len(df_b_out3))
    
    b_models = df_b_out3['model'].unique()
    b_models = list(b_models)
    b_models.remove('Historic')
    b_models.append('Historic')
    
    return df_b_out3, b_models


def get_demo_data():
    
    df = pd.read_csv('demo_data.zip', compression='zip')
    df['dt'] = pd.to_datetime(df['date'])
    
    b_models = df['model'].unique()
    b_models = list(b_models)
    b_models.remove('Historic')
    b_models.append('Historic')
    #print(b_models)
    return df, b_models



def get_df_us():
    
    #print(us.states.STATES)
    #print(us.states.STATES_AND_TERRITORIES)
    l = us.states.STATES_AND_TERRITORIES

    s_name=[]
    s_fips=[]
    s_abbr=[]
    for i in l:
        s_name.append(str(i))
        s_fips.append(i.fips)
        s_abbr.append(i.abbr)
        #print(i, i.abbr, i.fips)

    s_name.insert(0,'United States')
    s_fips.insert(0,'-1')
    s_abbr.insert(0,'US')

    df_us = pd.DataFrame({'State':s_name,'adm1':s_fips,'abbr':s_abbr})
    df_us['adm1'] = df_us['adm1'].astype(float)
    #df_us.sample(3)
    
    return df_us


# ------------------------






def define_dash_server(df, b_models, monday_str):

    # DASH Server 
    # Interrupt Kernel to stop server process  ( notebook shortcut:  select active CELL -> ESC, i,i )


    df_b_out = df

    df_sunday = df_b_out.loc[df_b_out['dt'].dt.weekday == 6]

    app = Dash(__name__)
    
    df_us = get_df_us()
    state_list = df_us['State']
    
    # OPTIONAL
    #random_state = df_us['State'].sample()
    #random_state = random_state.values[0]
    #print('---',random_state,'---')

    app.layout = html.Div([
        html.H3(children='Bucky Covid19 Forecast Model Explorer as of '+monday_str, 
                style={'padding-left':20,'padding-top':20,'padding-bottom':0,'margin-bottom':0,}),
        dcc.Graph(id="graph", style={'padding':10,'margin':0,'display':'block'},
                  config={'displayModeBar': False}),
        dcc.Checklist(
            id="checklist_models",
            options=b_models,
            value=[b_models[0],b_models[1],'Historic'],
            inline=True,
            labelStyle = {'display': 'block', 'cursor': 'pointer', 'margin-right':'20px'},
            style={'padding':0,'margin':20,'margin-top':0}
        ),
        dcc.Dropdown(['daily_reported_cases', 'daily_deaths', 'daily_hospitalizations'], 'daily_reported_cases', 
                     id='demo_dropdown', 
                     style={ 'display': 'inline-block', 'width':'200px', 'margin-top':0, 'margin-right':10, 'padding-left':20}),
        dcc.Dropdown(['98% Confidence Interval', '60% Confidence Interval', '50% Confidence Interval', '10% Confidence Interval'], '98% Confidence Interval', 
                     id='ci_dropdown', 
                     style={ 'display': 'inline-block', 'width':'230px', 'margin-top':0, 'margin-right':10,}),
        dcc.Dropdown(state_list, "United States", 
                     id='state_dropdown', 
                     style={ 'display': 'inline-block', 'width':'230px', 'margin-top':0}),
    ])


    @app.callback(
        Output("graph", "figure"), 
        Input("checklist_models", "value"),
        Input('demo_dropdown', 'value'),
        Input('ci_dropdown', 'value'),
        Input('state_dropdown', 'value'))

    
    def update_line_chart(models, demo, ci, state):
        #print('test', models, ' - ', demo, ' - ', ci, ' - ', state)
        df = df_b_out#[mask]

        df = df.loc[df['model'].isin(models)]

        df = df.loc[df['State'].isin([state])]

        input_y = demo

        df_hist = df.loc[df['model']=='Historic']

        df_hist_state = df_hist['State'].unique()
        if len(df_hist_state) > 0:
            df_hist_state = df_hist['State'].unique()[0]
        else:
            df_hist_state = 'Historic'


        layout = go.Layout(margin=go.layout.Margin(l=0,r=0,b=0,t=0))

        fig = go.Figure(layout=layout,)

        for i,v in enumerate(models):

            ii = b_models.index(v)
            vv = v[12:-3]
            offset = ii*6

            df_lower = df[df['model']==v]
            df_mid = df[df['model']==v]
            df_upper = df[df['model']==v]

            df_mid = df_mid[df_mid['quantile']==.5]

            if ci == '98% Confidence Interval':
                df_lower = df_lower[df_lower['quantile']==.01]
                df_upper = df_upper[df_upper['quantile']==.99]
                v_lower = '.01'
                v_upper = '.99'
            elif ci == '60% Confidence Interval':
                df_lower = df_lower[df_lower['quantile']==.2]
                df_upper = df_upper[df_upper['quantile']==.8]
                v_lower = '.20'
                v_upper = '.80'
            elif ci == '50% Confidence Interval':
                df_lower = df_lower[df_lower['quantile']==.25]
                df_upper = df_upper[df_upper['quantile']==.75]
                v_lower = '.25'
                v_upper = '.75'
            elif ci == '10% Confidence Interval':
                df_lower = df_lower[df_lower['quantile']==.45]
                df_upper = df_upper[df_upper['quantile']==.55]
                v_lower = '.45'
                v_upper = '.55'

            df_s_upper = df_upper.loc[df_upper['dt'].dt.weekday == 6]
            df_s_lower = df_lower.loc[df_lower['dt'].dt.weekday == 6]
            df_s_mid = df_mid.loc[df_mid['dt'].dt.weekday == 6]

            # MID
            fig.add_trace(go.Scatter(x=df_mid['date'], y=df_mid[input_y],
                fill=None,
                mode='lines',
                line=dict(color = colorss_02[18+offset], width=1.5),
                showlegend=False,
                name='.5 '+vv+' '+df_hist_state+' '+input_y,
                text=df_hist_state+' '+input_y,
                hovertemplate ='%{y:,.0f}',
                ))
            # UPPER
            fig.add_trace(go.Scatter(x=df_upper['date'], y=df_upper[input_y],
                fill=None,
                mode='lines',
                line=dict(color = colorss_02[20+offset], width=1.5),
                showlegend=False,
                name=v_upper+' '+vv+' '+df_hist_state+' '+input_y,
                text=df_hist_state+' '+input_y,
                hovertemplate ='%{y:,.0f}',
                ))

            # FILL QUANTILE RANGE
            fig.add_trace(go.Scatter(
                x=df_lower['date'],
                y=df_lower[input_y],
                fill='tonexty', # fill area between trace0 and trace1
                mode='lines', 
                line=dict(width=0),
                fillcolor=colorsss[ii],
                name=v_upper+' - '+v_lower+' Model '+vv+' '+df_hist_state,
                text=df_hist_state+' '+input_y,
                hovertemplate ='%{y:,.0f}', 
                ))

            # LOWER
            fig.add_trace(go.Scatter(x=df_lower['date'], y=df_lower[input_y],
                fill=None,
                mode='lines',
                line=dict(color = colorss_02[17+offset], width=1.5),
                showlegend=False,
                name=v_lower+' '+vv+' '+df_hist_state+' '+input_y,
                text=df_hist_state+' '+input_y,
                hovertemplate ='%{y:,.0f}', 
                ))

            # SUNDAY MARKERS
            c0 = np.stack((df_s_lower[input_y], df_s_upper[input_y]),axis=1)
            t0 = str(df_s_mid['quantile'].unique())+' - '+df_hist_state+' '+input_y
            fig.add_trace(go.Scatter(x=df_s_mid['date'], y=df_s_mid[input_y],
                name='.50 Sundays '+df_hist_state+' '+vv,
                mode='markers',
                text=t0,
                customdata=c0,
                marker=dict(
                    color=colorss_02[18+offset],size=4,
                    line=dict(color=colorss_02[18+offset],width=2)
                ),
                showlegend=True,
                hovertemplate =
                '<b>Bucky Forecast Model</b><br>'+
                '<b>'+v+'</b><br>'+
                input_y+'<br>'+
                '- - - - - - - - - - -<br>'+
                '%{x}<br>'+
                '<b>'+v_upper+'</b>: %{customdata[1]:,.0f}<br>'+
                '<b>.50</b>: %{y:,.0f}<br>'+
                '<b>'+v_lower+'</b>: %{customdata[0]:,.0f}<br>',
                ))

        # HIST
        fig.add_trace(go.Scatter(x=df_hist['date'], y=df_hist[input_y],
            fill=None,
            mode='lines',
            line=dict(color = colorss_02[5], width=2.5),
            name="Historic "+str(df_hist_state)+' '+input_y,
            text=str(df_hist_state),
            hovertemplate ='%{y:,.0f}',
            ))


        fig.add_vline(x=sunday_str, line_width=.5, line_dash="dash", line_color="red")
        fig.update_traces(hoverlabel_namelength=-1,hoveron='points' )

        return fig
    
    
    print('Starting Dash Server.')
    app.run_server(debug=True, use_reloader=False)
    
    return



def start_dash():
    print('Model Explorer.')
    print('-----------------')
    
    bucky_data_path = '/mnt/c/Users/smolijm1/Desktop/python/bucky-output/'
    reichlab_data_path = '/mnt/c/Users/smolijm1/Desktop/python/covid19-forecast-hub/'
    #monday_str = '2022-08-15'
    
    df, b_models = prepare_data(bucky_data_path, reichlab_data_path, monday_str)
    print('Local Data loaded.')
    
    define_dash_server(df, b_models, monday_str)
    
    return


def start_dash_demo():
    print('Demo Model Explorer.')
    print('-----------------')
    
    df, b_models = get_demo_data()
    print('Demo Data loaded.')
    
    define_dash_server(df, b_models, monday_str)
        
    return


if __name__ == '__main__':
    print('Bucky Model Viewer')
    print('-------------------')
    print('1 - Demo data')
    print('2 - Local data')
    print('')
    x = input('Enter an option : ')
    if x == '1':
        print('Run Demo.')
        start_dash_demo()
    elif x == '2':
        print('Run Local.')
        print('Local data paths and RUN DATE may need to be configured in PREPARE_DATA() function.')
        print('Data preprocess may take a few seconds.')
        print('CTRL+C twice to stop server.')
        print('If the data paths have not been set to the Bucky Model Output and Reigh Lab data repositories, use the demo data file.')
        print('')
        y = input('Enter to Continue.')
        start_dash()