from typing import Dict, List
from models.classes import BitQueryAPI
from utils.base_helpers import safe_print
from config.settings import print_lock, GLOBAL_USD_THRESHOLD
from data.tokens import TOKENS_TO_MONITOR

def monitor_bitquery_transfers():
    """Monitor token transfers using BitQuery with enhanced analytics"""
    bitquery = BitQueryAPI()
    
    try:
        # Monitor cross-chain transfers
        cross_chain_query = """
        query ($threshold: Float!) {
          ethereum: ethereum {
            transfers: dexTrades(
              options: {limit: 100, desc: "block.timestamp"}
              amount: {gt: $threshold}
            ) {
              transaction { hash }
              date { date }
              buyCurrency { symbol, address }
              sellCurrency { symbol, address }
              buyAmount
              sellAmount
              tradeAmount(in: USD)
            }
          }
          bsc: bsc {
            transfers: dexTrades(
              options: {limit: 100, desc: "block.timestamp"}
              amount: {gt: $threshold}
            ) {
              transaction { hash }
              date { date }
              buyCurrency { symbol, address }
              sellCurrency { symbol, address }
              buyAmount
              sellAmount
              tradeAmount(in: USD)
            }
          }
        }
        """
        
        variables = {"threshold": GLOBAL_USD_THRESHOLD / 1000}
        results = bitquery.execute_query(cross_chain_query, variables)
        
        if results and 'data' in results:
            # Process Ethereum transfers
            eth_transfers = results['data']['ethereum']['transfers']
            safe_print("\n=== Ethereum Large Transfers ===")
            process_chain_transfers(eth_transfers, "Ethereum")
            
            # Process BSC transfers
            bsc_transfers = results['data']['bsc']['transfers']
            safe_print("\n=== BSC Large Transfers ===")
            process_chain_transfers(bsc_transfers, "BSC")
            
        # Monitor token holder changes
        holder_query = """
        query ($tokens: [String!]) {
          ethereum {
            transfers(
              options: {limit: 50, desc: "block.timestamp"}
              currency: {in: $tokens}
            ) {
              currency { symbol, address }
              sender { address }
              receiver { address }
              amount
              transaction { hash }
              block { timestamp }
            }
          }
        }
        """
        
        token_addresses = [info["contract"] for info in TOKENS_TO_MONITOR.values()]
        holder_results = bitquery.execute_query(holder_query, {"tokens": token_addresses})
        
        if holder_results and 'data' in holder_results:
            transfers = holder_results['data']['ethereum']['transfers']
            safe_print("\n=== Token Holder Changes ===")
            for transfer in transfers:
                symbol = transfer['currency']['symbol']
                amount = float(transfer['amount'])
                safe_print(
                    f"• {symbol}: {amount:,.2f} tokens | "
                    f"From: {transfer['sender']['address'][:10]}... | "
                    f"To: {transfer['receiver']['address'][:10]}..."
                )

    except Exception as e:
        safe_print(f"Error monitoring BitQuery transfers: {e}")

def process_chain_transfers(transfers: List[Dict], chain: str):
    """Process transfers for a specific chain"""
    for transfer in transfers:
        buy_symbol = transfer['buyCurrency']['symbol']
        sell_symbol = transfer['sellCurrency']['symbol']
        buy_amount = float(transfer['buyAmount'])
        sell_amount = float(transfer['sellAmount'])
        usd_amount = float(transfer['tradeAmount'])
        
        safe_print(
            f"• {chain}: {sell_amount:,.2f} {sell_symbol} → "
            f"{buy_amount:,.2f} {buy_symbol} | "
            f"Value: ${usd_amount:,.2f}"
        )