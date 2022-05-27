import os
import sys
import traceback
import json
import jwt
import requests
import pickle
import pandas as pd
#import plotly.express as px
#mport dash
#mport dash_core_components as dcc
#import dash_html_components as html
import xml.etree.ElementTree as ET

#from dash.dependencies import Input, Output

from datetime import date
from os import environ
from flask import Flask, render_template, request, send_from_directory, jsonify, url_for
from flask_sqlalchemy import SQLAlchemy

from urllib.request import urlopen
app = Flask(__name__)

is_prod = os.environ.get('IS_HEROKU', None)

demo_account_key = '14afe305132a682a2742743df532707d'
sector = 'technology'
marketCap = '100000000000'
limit = '100'

if is_prod:

    DATABASE_CONNECTION_STRING = 'postgresql://postgres:Salesforce@1@localhost/lexus'

    app.debug = False
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgres://thiweluidvxhpu:20b2eb1acad565d40001708b87df8c0471964611626779c8ea956fcb478025c3@ec2-52-20-248-222.compute-1.amazonaws.com:5432/dmu98l1605ata?sslmode=require'

else:
    from config_dev import Config
    app.config.from_object(Config)

    DEBUG = Config.DEBUG
    app.debug = DEBUG
    app.config['SQLALCHEMY_DATABASE_URI'] = Config.DATABASE_CONNECTION_STRING

TEST_ENDPOINT_URL = "https://en9wlwumpvrjw.x.pipedream.net"


SITE_ROOT = os.path.realpath(os.path.dirname(__file__))

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


class APICall(db.Model):
    cloud_api_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    update_date = db.Column(db.DateTime)

    def __init__(self,name, update_date):
        self.name = name
        self.update_date = update_date


@app.route('/technology.html', methods=['GET','POST'])
def index_html():
    path_to_css = os.path.join(request.url_root, "static/")

    try:
        api_query_date_obj = db.session.query(APICall).filter(APICall.name=='FMP Cloud API')
        if(api_query_date_obj.count() == 0):
            api_loaded_today = False
        else:
            api_loaded_today = api_query_date_obj[0].update_date.date() == date.today()
    except:
        api_loaded_today = False

    #api_loaded_today = False

    #if the api call was called today, get the output from the serialized pickle files. Otherwise, call the api to get the output
    if(api_loaded_today):
        companies, balancesheet_list, incomestatemnt_list, cashflowstatemnt_list, marketcap_data_list, companydata_list, technological_companies = load_data_from_pickle()
    else:
        companies, balancesheet_list, incomestatemnt_list, cashflowstatemnt_list, marketcap_data_list, companydata_list, technological_companies = load_data_from_api(sector, marketCap, limit, demo_account_key)

    company_details = {}
    profitability_ratios = {}
    activity_ratios = {}
    solvency_ratios = {}
    dupont_identity = {}
    stock_details = {}

    res = [ele for ele in marketcap_data_list if ele != []]
    
    for item in res:

        balancesheet = []
        incomestatemnt = []

        symbol = item[0]['symbol']

        marketcap = item[0]['marketCap']

        for comp in companydata_list:
            if(comp[0]['symbol'] == symbol):

                latest_Annual_Dividend = comp[0]['lastDiv']
                price = comp[0]['price']
                company_name = comp[0]['companyName']

        for bsl in balancesheet_list:
            for bsl_item in bsl:
                if(bsl_item['symbol'] == symbol):
                    balancesheet.append(bsl_item)

        for istm in incomestatemnt_list:
            for istm_item in istm:
                if(istm_item['symbol'] == symbol):
                    incomestatemnt.append(istm_item)


        ##---------

        try:
            net_income_last_4_quarters = incomestatemnt[0]['netIncome']
        except:
            pass

        try:
            revenue_last_4_quarters = incomestatemnt[0]['revenue']
        except:
            pass

        try:
            cost_of_revenue_last_4_quarters = incomestatemnt[0]['costOfRevenue']
        except:
            pass

        try:
            interest_expense_last_4_quarters = incomestatemnt[0]['interestExpense']
        except:
            pass

        try:
            operating_income_last_4_quarters = incomestatemnt[0]['operatingIncome']
        except:
            pass

        try:
            gross_profit_last_4_quarters = incomestatemnt[0]['grossProfit']
        except:
            pass

        try:
            operating_expenses_last_4_quarters = incomestatemnt[0]['operatingExpenses']
        except:
            pass


        try:
            #ebit_last_4_quarters = (incomestatemnt[0]['ebitda'] - (incomestatemnt[0]['depreciationAndAmortization'] + incomestatemnt[0]['interestExpense']))
            ebit_last_4_quarters = incomestatemnt[0]['netIncome'] + incomestatemnt[0]['interestExpense'] + incomestatemnt[0]['incomeTaxExpense']
        except:
            pass

        ##---------

        try:
            net_income_last_4_quarters += incomestatemnt[1]['netIncome']
        except:
            pass

        try:
            revenue_last_4_quarters += incomestatemnt[1]['revenue']
        except:
            pass

        try:
            cost_of_revenue_last_4_quarters += incomestatemnt[1]['costOfRevenue']
        except:
            pass

        try:
            interest_expense_last_4_quarters += incomestatemnt[1]['interestExpense']
        except:
            pass

        try:
            operating_income_last_4_quarters += incomestatemnt[1]['operatingIncome']
        except:
            pass

        try:
            gross_profit_last_4_quarters += incomestatemnt[1]['grossProfit']
        except:
            pass

        try:
            operating_expenses_last_4_quarters += incomestatemnt[1]['operatingExpenses']
        except:
            pass

        try:
            #ebit_last_4_quarters += (incomestatemnt[1]['ebitda'] - (incomestatemnt[1]['depreciationAndAmortization'] + incomestatemnt[1]['interestExpense']))
            ebit_last_4_quarters += incomestatemnt[1]['netIncome'] + incomestatemnt[1]['interestExpense'] + incomestatemnt[1]['incomeTaxExpense']

        except:
            pass

        ##---------

        try:
            net_income_last_4_quarters += incomestatemnt[2]['netIncome']
        except:
            pass

        try:
            revenue_last_4_quarters += incomestatemnt[2]['revenue']
        except:
            pass

        try:
            cost_of_revenue_last_4_quarters += incomestatemnt[2]['costOfRevenue']
        except:
            pass

        try:
            interest_expense_last_4_quarters += incomestatemnt[2]['interestExpense']
        except:
            pass

        try:
            operating_income_last_4_quarters += incomestatemnt[2]['operatingIncome']
        except:
            pass

        try:
            gross_profit_last_4_quarters += incomestatemnt[2]['grossProfit']
        except:
            pass

        try:
            operating_expenses_last_4_quarters += incomestatemnt[2]['operatingExpenses']
        except:
            pass

        try:
            #ebit_last_4_quarters += (incomestatemnt[2]['ebitda'] - (incomestatemnt[2]['depreciationAndAmortization'] + incomestatemnt[2]['interestExpense']))
            ebit_last_4_quarters += incomestatemnt[2]['netIncome'] + incomestatemnt[2]['interestExpense'] + incomestatemnt[2]['incomeTaxExpense']

        except:
            pass

        ##---------

        try:
            net_income_last_4_quarters += incomestatemnt[3]['netIncome']
        except:
            pass

        try:
            revenue_last_4_quarters += incomestatemnt[3]['revenue']
        except:
            pass

        try:
            cost_of_revenue_last_4_quarters += incomestatemnt[3]['costOfRevenue']
        except:
            pass

        try:
            interest_expense_last_4_quarters += incomestatemnt[3]['interestExpense']
        except:
            pass

        try:
            operating_income_last_4_quarters += incomestatemnt[3]['operatingIncome']
        except:
            pass

        try:
            gross_profit_last_4_quarters += incomestatemnt[3]['grossProfit']
        except:
            pass

        try:
            operating_expenses_last_4_quarters += incomestatemnt[3]['operatingExpenses']
        except:
            pass

        try:
            #ebit_last_4_quarters += (incomestatemnt[3]['ebitda'] - (incomestatemnt[3]['depreciationAndAmortization'] + incomestatemnt[3]['interestExpense']))
            ebit_last_4_quarters += incomestatemnt[3]['netIncome'] + incomestatemnt[3]['interestExpense'] + incomestatemnt[3]['incomeTaxExpense']

        except:
            pass
        ##---------

        """
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #------------------------------------------------------ COMPANY DETAILS -----------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        """

        company_details[symbol] = {}
        company_details[symbol]['Company Name'] = company_name

        """
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------- PROFITABILITY RATIOS -------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        """

        # PROFITABILITY RATIO:  How much profit for each dollar of shareholders equity?
        try:
            ROE = (net_income_last_4_quarters / ((balancesheet[0]['totalStockholdersEquity']+ balancesheet[3]['totalStockholdersEquity'])/2))
        except:
            ROE = (net_income_last_4_quarters / (balancesheet[0]['totalStockholdersEquity']/2))

        # PROFITABILITY RATIO: How much profit for each dollar of assets the company owns?        
        try:
            ROA = (net_income_last_4_quarters / ((balancesheet[0]['totalAssets']+ balancesheet[3]['totalAssets'])/2))
        except:
            ROA = (net_income_last_4_quarters / (balancesheet[0]['totalAssets']/2))

        # PROFITABILITY RATIO:  How the company has managed its direct costs incurred during delivery of products/services. Needs to be as high as possible. 
        gross_profit_margin = (revenue_last_4_quarters - cost_of_revenue_last_4_quarters)/revenue_last_4_quarters

        # PROFITABILITY RATIO: Operating Profit Margin
        operating_profit_margin = (ebit_last_4_quarters / revenue_last_4_quarters)

        # PROFITABILITY RATIO: Pretax Margin
        pretax_margin = (ebit_last_4_quarters - interest_expense_last_4_quarters)/revenue_last_4_quarters

        # PROFITABILITY RATIO: Net Profit Margin
        net_profit_margin = net_income_last_4_quarters/revenue_last_4_quarters

        price_to_sales = marketcap / (revenue_last_4_quarters)
        price_to_earnings = marketcap / (net_income_last_4_quarters) 
        price_to_book = marketcap / balancesheet[0]['totalStockholdersEquity']


        profitability_ratios[symbol] = {}
        profitability_ratios[symbol]['Company Name'] = company_name

        profitability_ratios[symbol]['ROE'] = str(round(ROE * 100,2)) + '%' 
        profitability_ratios[symbol]['ROA'] = str(round(ROA * 100,2)) + '%'
        profitability_ratios[symbol]['Gross Profit Margin'] = str(round(gross_profit_margin * 100,2)) + '%' 
        profitability_ratios[symbol]['Operating Profit Margin'] = str(round(operating_profit_margin*100,2)) + '%' 
        profitability_ratios[symbol]['Pretax Margin'] = str(round(pretax_margin*100,2)) + '%' 
        profitability_ratios[symbol]['Net Profit Margin'] = str(round(net_profit_margin*100,2)) + '%' 

        """
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #---------------------------------------------------------- ACTIVITY RATIOS -------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        """

        #ACTIVITY RATIO: Total Asseet Turnover
        try:
            total_asset_turnover = revenue_last_4_quarters/((balancesheet[0]['totalAssets'] + balancesheet[3]['totalAssets'])/2)
        except:
            total_asset_turnover = revenue_last_4_quarters/balancesheet[0]['totalAssets']

        #ACTIVITY RATIO: Fixed Asset Turnover
        try:
            fixed_asset_turnover = revenue_last_4_quarters/((balancesheet[0]['propertyPlantEquipmentNet'] + balancesheet[3]['propertyPlantEquipmentNet'])/2)
        except:

            if(balancesheet[0]['propertyPlantEquipmentNet'] > 0.0):
                fixed_asset_turnover = revenue_last_4_quarters/balancesheet[0]['propertyPlantEquipmentNet']
            else:
                fixed_asset_turnover = 0.0

        #ACTIVITY RATIO: Working Capital Turnover
        current_assets_0 = (balancesheet[0]['totalCurrentAssets'] - balancesheet[0]['totalCurrentLiabilities'])

        try:
            current_assets_1 = (balancesheet[3]['totalCurrentAssets'] - balancesheet[3]['totalCurrentLiabilities'])
        except:
            current_assets_1 = 0.0

        working_capital_turnover = revenue_last_4_quarters/((current_assets_0 + current_assets_1)/2)

        #ACTIVITY RATIO: Days Receivables Outstanding
        try:
            days_receivables_outstanding = revenue_last_4_quarters/((balancesheet[0]['netReceivables'] + balancesheet[3]['netReceivables'])/2)
        except:
            days_receivables_outstanding = revenue_last_4_quarters/balancesheet[0]['netReceivables']

        days_receivables_outstanding = 365/days_receivables_outstanding    

        #ACTIVITY RATIO: Days Inventory Held
        try:
            days_inventory_held = cost_of_revenue_last_4_quarters/((balancesheet[0]['inventory'] + balancesheet[3]['inventory'])/2)
            days_inventory_held = 365/days_inventory_held    

        except:
            try:
                days_inventory_held = cost_of_revenue_last_4_quarters/balancesheet[0]['inventory']
                days_inventory_held = 365/days_inventory_held    
            except:
                days_inventory_held = 0.0

        #ACTIVITY RATIO: Days Payable Outstanding
        try:
            net_credit_purchases = incomestatemnt[0]['costOfRevenue'] + balancesheet[0]['inventory'] - balancesheet[3]['inventory'] 
        except:
            net_credit_purchases = incomestatemnt[0]['costOfRevenue'] + balancesheet[0]['inventory'] 

        try:
            days_payable_outstanding = net_credit_purchases/((balancesheet[0]['accountPayables'] + balancesheet[3]['accountPayables'])/2)
            days_payable_outstanding = 365/days_payable_outstanding

        except:
            try:
                days_payable_outstanding = net_credit_purchases/balancesheet[0]['accountPayables']
                days_payable_outstanding = 365/days_payable_outstanding
            except:
                days_payable_outstanding = 0.0


        cash_conversion_cycle = days_receivables_outstanding + days_inventory_held - days_payable_outstanding

        activity_ratios[symbol] = {}
        #activity_ratios[symbol]['Company Name'] = company_name
        activity_ratios[symbol]['Total Asset Turnover'] = str(round(total_asset_turnover,2))
        activity_ratios[symbol]['Fixed Asset Turnover'] = str(round(fixed_asset_turnover,2))
        activity_ratios[symbol]['Working Capital Turnover'] = str(round(working_capital_turnover,2))
        activity_ratios[symbol]['Days Receivables Outstanding'] = str(round(days_receivables_outstanding,2)) + ' days'
        activity_ratios[symbol]['Days Inventory Held'] = str(round(days_inventory_held,2)) + ' days'
        activity_ratios[symbol]['Days Payable Outstanding'] = str(round(days_payable_outstanding,2)) + ' days'
        activity_ratios[symbol]['Cash Conversion Cycle'] = str(round(cash_conversion_cycle,2)) + ' days'


        """
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #---------------------------------------------------------- SOLVENCY RATIOS -------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        """

        #SOLVENCY RATIO: Debt to Equity. How much long term debt for each dollar of shareholder equity
        debt_to_equity = balancesheet[0]['longTermDebt'] / balancesheet[0]['totalStockholdersEquity']

        #SOLVENCY RATIO: Total Liabilities to Total Assets. Every 1 dollar in assets, how much comes from liabilities (v equity capital?)
        total_liabilitiues_to_total_assets = balancesheet[0]['totalDebt'] / balancesheet[0]['totalAssets']

        #SOLVENCY RATIO: Interest Coverage Ratio.
        try:    
            interest_coverage_ratio = ebit_last_4_quarters / interest_expense_last_4_quarters
        except:

            interest_coverage_ratio = 0.0

        #SOLVENCY RATIO: Current Ratio. Can the companys current assets meet its current liabilities
        current_ratio = balancesheet[0]['totalCurrentAssets'] / balancesheet[0]['totalCurrentLiabilities']

        #SOLVENCY RATIO: Quick Ratio. Can the companys current assets meet its current liabilities - Adjustment for ignoring inventory
        quick_ratio = (balancesheet[0]['totalCurrentAssets'] - balancesheet[0]['inventory']) / balancesheet[0]['totalCurrentLiabilities']

        #SOLVENCY RATIO: Quick Ratio. Only considers cash and cash equivalents as current assets
        cash_ratio = balancesheet[0]['cashAndCashEquivalents'] / balancesheet[0]['totalCurrentLiabilities']

        #solvency_ratios[symbol]['Company Name'] = company_name
        solvency_ratios[symbol] = {}
        solvency_ratios[symbol]['Debt to Equity'] = round(debt_to_equity,3)
        solvency_ratios[symbol]['Total Liabilities to Total Assets'] = round(total_liabilitiues_to_total_assets,3)
        solvency_ratios[symbol]['Interest Coverage Ratio'] = round(interest_coverage_ratio,3)         
        solvency_ratios[symbol]['Current Ratio'] = round(current_ratio,3) 
        solvency_ratios[symbol]['Quick Ratio'] = round(quick_ratio,3) 
        solvency_ratios[symbol]['Cash Ratio'] = round(cash_ratio,3) 

        """
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #---------------------------------------------------------- DuPONT IDENTITY -------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        """

        try:
            average_shareholders_equity = (balancesheet[0]['totalStockholdersEquity']+ balancesheet[3]['totalStockholdersEquity'])/2
        except:
            average_shareholders_equity = balancesheet[0]['totalStockholdersEquity']

        try:
            average_total_assets = (balancesheet[0]['totalAssets'] + balancesheet[3]['totalAssets'])/2
        except:
            average_total_assets = balancesheet[0]['totalAssets']

        equity_multiplier = average_total_assets/average_shareholders_equity
        back_to_ROE = net_profit_margin * total_asset_turnover * equity_multiplier


        """
        Net Profit Margin - how well the company is doing in controlling its expenses, leading to higher profits
        Total Asset Turnover - has efficiency increased or decreased? How efficient is the company in turning over its assets?
        Equity Multiplier - Increase indicates that the company has borrowed more debt capital than raising equity capital. 
        """

        dupont_identity[symbol] = {}
        dupont_identity[symbol]['Net Profit Margin'] = str(round(net_profit_margin * 100,2)) + '%'
        dupont_identity[symbol]['Total Asset Turnover'] = round(total_asset_turnover * 100,2)
        dupont_identity[symbol]['Equity Multiplier'] = round(equity_multiplier,2)         
        dupont_identity[symbol]['Equity Multiplier'] = str(round(back_to_ROE,2))         

        """
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #---------------------------------------------------------- STOCK DETAILS ---------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        #----------------------------------------------------------------------------------------------------------------------------------#
        """

        """
        PE Ratio - How much investors are willing to pay for each dollar of profits the company makes

        """

        dividend_yield = latest_Annual_Dividend / price
        pe_ratio =  price/incomestatemnt[0]['eps'] 

        #import pdb; pdb.set_trace()



        stock_details[symbol] = {}
        stock_details[symbol]['Dividend Yield'] = str(round(dividend_yield * 100,2)) + '%'
        stock_details[symbol]['PE Ratio'] = str(round(pe_ratio,2))

        # ****TODO**** DCF, FCM and other models for calculating stock price

        stock_details[symbol]['Current Share Price'] = 'USD ' + str(round(price,2))

        """
        if(symbol == 'AAPL'):
            print()
            print()
            print()
            print("ROE: " +  str(round(ROE * 100,2)) + '%' )
            print("Net Profit Margin: " +  str(round(net_profit_margin * 100,2)) + '%')
            print("Total Asset Turnover: " +  str(round(total_asset_turnover * 100,2)))
            print("Average Shareholders Equity: " +  str(round(average_shareholders_equity * 100,2)))
            print("Average Total Assets: " +  str(round(average_total_assets * 100,2)) + '%')
            print("Equity Multiplier: " + str(round(equity_multiplier * 100,2)))
            print()
            print()
            print()

            back_to_ROE = net_profit_margin * total_asset_turnover * equity_multiplier

            print("RETURN ROE: " +  str(round(back_to_ROE * 100,2)) + '%' )
        """
        
    company_details_df = pd.DataFrame.from_dict(company_details, orient='index')
    company_details_df = company_details_df.T
    company_details_html = company_details_df.to_html()

     
    profitability_ratios_df = pd.DataFrame.from_dict(profitability_ratios, orient='index')
    profitability_ratios_df = profitability_ratios_df.T
    #metrics_df['mean'] = round(metrics_df.mean(axis=1),3)

    profitability_ratios_html = profitability_ratios_df.to_html()


    activity_ratios_df = pd.DataFrame.from_dict(activity_ratios, orient='index')
    activity_ratios_df = activity_ratios_df.T
    activity_ratios_html = activity_ratios_df.to_html()

    solvency_ratios_df = pd.DataFrame.from_dict(solvency_ratios, orient='index')
    solvency_ratios_df = solvency_ratios_df.T
    solvency_ratios_html = solvency_ratios_df.to_html()

    dupont_identity_df = pd.DataFrame.from_dict(dupont_identity, orient='index')
    dupont_identity_df = dupont_identity_df.T
    dupont_identity_html = dupont_identity_df.to_html()

    stock_details_df = pd.DataFrame.from_dict(stock_details, orient='index')
    stock_details_df = stock_details_df.T
    stock_details_html = stock_details_df.to_html()


    return render_template('index.html', company_details_html=company_details_html,profitability_ratios_html=profitability_ratios_html, activity_ratios_html=activity_ratios_html, solvency_ratios_html=solvency_ratios_html, dupont_identity_html=dupont_identity_html,stock_details_html=stock_details_html, css_path=path_to_css)


def load_data_from_api(sector, marketCap, limit, demo_account_key):
    
    companies = requests.get(f'https://fmpcloud.io/api/v3/stock-screener?sector={sector}&marketCapMoreThan={marketCap}&limit={limit}&apikey={demo_account_key}').json()
    
    technological_companies = []
    balancesheet_list = []
    incomestatemnt_list = []
    cashflowstatemnt_list = []
    marketcap_data_list = []
    companydata_list = []
    
    for item in companies:
        technological_companies.append(item['symbol'])

    for item in technological_companies:
        try:

            balancesheet_list.append(requests.get(f'https://fmpcloud.io/api/v3/balance-sheet-statement/{item}?period=quarter&limit=10&apikey={demo_account_key}').json())
            incomestatemnt_list.append(requests.get(f'https://fmpcloud.io/api/v3/income-statement/{item}?period=quarter&limit=10&apikey={demo_account_key}').json())
            cashflowstatemnt_list.append(requests.get(f'https://fmpcloud.io/api/v3/cash-flow-statement/{item}?period=quarter&limit=10&apikey={demo_account_key}').json())
            marketcap_data_list.append(requests.get(f'https://fmpcloud.io/api/v3/market-capitalization/{item}?apikey={demo_account_key}').json())
            companydata_list.append(requests.get(f'https://fmpcloud.io/api/v3/profile/{item}?apikey={demo_account_key}').json())

        except:
            pass
    
    
    #pickle the data

    pickle_out = open("companies.pickle", "wb")
    pickle.dump(companies,pickle_out)
    pickle_out.close()

    pickle_out = open("technological_companies.pickle", "wb")
    pickle.dump(technological_companies,pickle_out)
    pickle_out.close()

    pickle_out = open("incomestatemnt.pickle", "wb")
    pickle.dump(incomestatemnt_list,pickle_out)
    pickle_out.close()

    pickle_out = open("balancesheet.pickle", "wb")
    pickle.dump(balancesheet_list,pickle_out)
    pickle_out.close()

    pickle_out = open("cashflowstatemnt.pickle", "wb")
    pickle.dump(cashflowstatemnt_list,pickle_out)
    pickle_out.close()

    pickle_out = open("marketcap_data.pickle", "wb")
    pickle.dump(marketcap_data_list,pickle_out)
    pickle_out.close()

    pickle_out = open("companydata.pickle", "wb")
    pickle.dump(companydata_list,pickle_out)
    pickle_out.close()
    
    #set database timestamp for FMP Cloud API
    if db.session.query(APICall).filter(APICall.name == 'FMP Cloud API').count() == 0:
        data = APICall("FMP Cloud API", date.today())
        db.session.add(data)
        db.session.commit()
    else:
        data = db.session.query(APICall).filter(APICall.name=='FMP Cloud API').update({APICall.update_date: date.today()})
        db.session.commit()

    #return companies
    return companies, balancesheet_list, incomestatemnt_list, cashflowstatemnt_list, marketcap_data_list, companydata_list, technological_companies

def load_data_from_pickle():

    pickle_in = open("companies.pickle","rb")
    companies = pickle.load(pickle_in)

    pickle_in = open("technological_companies.pickle","rb")
    technological_companies = pickle.load(pickle_in)

    pickle_in = open("balancesheet.pickle","rb")
    balancesheet_list = pickle.load(pickle_in)

    pickle_in = open("incomestatemnt.pickle","rb")
    incomestatemnt_list = pickle.load(pickle_in)

    pickle_in = open("cashflowstatemnt.pickle","rb")
    cashflowstatemnt_list = pickle.load(pickle_in)

    pickle_in = open("marketcap_data.pickle","rb")
    marketcap_data_list = pickle.load(pickle_in)

    pickle_in = open("companydata.pickle","rb")
    companydata_list = pickle.load(pickle_in)

    return companies, balancesheet_list, incomestatemnt_list, cashflowstatemnt_list, marketcap_data_list, companydata_list, technological_companies



if __name__ == '__main__':
    app.run()