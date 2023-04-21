from subgrounds import Subgrounds
from dotenv import load_dotenv
import os
import pandas as pd

# Load env files
load_dotenv()
api_key = os.getenv('GRAPH_API_KEY')

# Load Subgrounds instance
sg = Subgrounds()

# Load subgraphs
univ3 = sg.load_subgraph(f'https://gateway.thegraph.com/api/{api_key}/subgraphs/id/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7')

# Variables
gno = '0x6810e776880c02933d47db1b9fc05908e5386b96'.lower()

# Create a query which fetches alls swaps involving GNO
swaps = univ3.Query.swaps(
    first = 10000,
    where = {
        'tokenIn': '0x6810e776880c02933d47db1b9fc05908e5386b96'
    },
    orderBy = 'timestamp',
    orderDirection = 'desc',
)

df = sg.query_df([
    swaps.timestamp,
    swaps.hash,
    swaps.tokenIn.symbol,
    swaps.amountIn,
    swaps.tokenOut.symbol,
    swaps.amountOut,
    swaps.amountInUSD,
])

df.to_csv('sells.csv', index=False)

# Create a query which fetches alls swaps involving GNO
swaps = univ3.Query.swaps(
    first = 10000,
    where = {
        'tokenOut': '0x6810e776880c02933d47db1b9fc05908e5386b96'
    },
    orderBy = 'timestamp',
    orderDirection = 'desc',
)

df = sg.query_df([
    swaps.timestamp,
    swaps.hash,
    swaps.tokenIn.symbol,
    swaps.amountIn,
    swaps.tokenOut.symbol,
    swaps.amountOut,
    swaps.amountInUSD,
])

df.to_csv('buys.csv', index=False)