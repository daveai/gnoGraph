from subgrounds import Subgrounds
from dotenv import load_dotenv
import os
import pandas as pd

def fetch(endpoint, label):
    # Create a query which fetches all sells involving GNO
    swaps = endpoint.Query.swaps(
        first = 20000,
        where = {
            'tokenIn': '0x6810e776880c02933d47db1b9fc05908e5386b96'
        },
        orderBy = 'timestamp',
        orderDirection = 'desc',
    )
    
    # Query
    sells = query(swaps)

    # Create a query which fetches all buys involving GNO
    swaps = univ3.Query.swaps(
        first = 20000,
        where = {
            'tokenOut': '0x6810e776880c02933d47db1b9fc05908e5386b96',
        },
        orderBy = 'timestamp',
        orderDirection = 'desc',
    )
    
    # Query
    buys = query(swaps)
    
    # Merge dataframes
    df = pd.concat([sells, buys])
    df['dex'] = label
    
    return df

def query(swaps):
    data = sg.query_df([
        swaps.timestamp,
        swaps.hash,
        swaps.__getattribute__('from'),
        swaps.amountIn,
        swaps.tokenIn.symbol,
        swaps.tokenIn.decimals,
        swaps.amountInUSD,
        swaps.amountOut,
        swaps.tokenOut.symbol,
        swaps.tokenOut.decimals,
        swaps.amountOutUSD,
    ])
    
    return data

def clean(df):
    # Turn amountIn from wei to eth based on tokenIn decimals
    df['swaps_amountIn'] = df['swaps_amountIn'] / 10**df['swaps_tokenIn_decimals']
    # Turn amountOut from wei to eth based on tokenOut decimals
    df['swaps_amountOut'] = df['swaps_amountOut'] / 10**df['swaps_tokenOut_decimals']
    
    # Turn timestamp to datetime
    df['swaps_timestamp'] = pd.to_datetime(df['swaps_timestamp'], unit='s')
    
    # Drop decimals columns
    df.drop(['swaps_tokenIn_decimals', 'swaps_tokenOut_decimals'], axis=1, inplace=True)
    
    return df

if __name__ == '__main__':
    # Load env files
    load_dotenv()
    api_key = os.getenv('GRAPH_API_KEY')
    
    # Load Subgrounds instance
    sg = Subgrounds()

    # Load subgraphs
    univ3 = sg.load_subgraph(f'https://gateway.thegraph.com/api/{api_key}/subgraphs/id/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7')
    balv2 = sg.load_subgraph(f'https://gateway.thegraph.com/api/{api_key}/subgraphs/id/Ei5typKWPepPSgqkaKf3p5bPhgJesnu1RuRpyt69Pcrx')
    
    uni = fetch(univ3, 'uniswap')
    bal = fetch(balv2, 'balancer')
    
    # Merge and clean dataframes
    df = pd.concat([uni, bal])
    df = clean(df)
    df.to_csv('agg_trades.csv', index=False)
    
    
    