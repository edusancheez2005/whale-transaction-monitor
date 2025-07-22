
# PROMPT FOR CLAUDE SONNET 4: Comprehensive Whale Transaction Classification Enhancement

## 🎯 High-Level Objective

Your mission is to intelligently refactor and enhance the Whale Transaction Intelligence Engine. The primary goal is to **increase the BUY/SELL signal classification rate from the current 20% to a target of 60-70%** while maintaining high accuracy. This involves moving beyond simple CEX/DEX detection and building a more nuanced, context-aware system that minimizes "TRANSFER" classifications for actionable economic events.

## 现状 (Current State) & The Core Problem

The system uses a sophisticated, multi-phase pipeline (`WhaleIntelligenceEngine` in `utils/classification_final.py`) to classify transactions. However, its logic is overly conservative:
1.  **Restrictive Confidence Thresholds**: High thresholds (75-85%) for early exits and master classification cause many potentially valid signals to be discarded.
2.  **Default to TRANSFER**: When faced with ambiguity (e.g., token-to-token swaps, bridge transactions, unrecognized protocols), the system defaults to "TRANSFER," losing valuable intelligence.
3.  **Limited Protocol & Pattern Recognition**: The logic doesn't fully capture the economic intent behind complex DeFi interactions like yield farming, advanced staking, or liquidity providing.

## 🛠️ CORE ENHANCEMENT AREAS & ACTIONABLE TASKS

You are to implement the following enhancements, focusing your changes primarily within `utils/classification_final.py` and `config/settings.py`.

### A. Confidence Threshold & Aggregation Logic Overhaul

The current system is too rigid. We need a more flexible confidence model.

1.  **Lower Core Thresholds**: In `config/settings.py`, adjust `CLASSIFICATION_THRESHOLDS` to be more permissive:
    *   `high_confidence`: `0.85` -> `0.80`
    *   `medium_confidence`: `0.70` -> `0.60`
    *   `early_exit_threshold`: `0.90` -> `0.85`
2.  **Adjust Early Exit Logic**: In the `_should_exit_early` method in `classification_final.py`, lower the confidence requirements:
    *   CEX early exit: `0.85` -> `0.75`
    *   DEX/Protocol early exit: `0.75` -> `0.70`
3.  **Implement Confidence Stacking**: In `_determine_master_classification`, enhance the weighted aggregation. **Do not just average scores.** Implement a system where multiple "medium-confidence" signals can combine to create one "high-confidence" signal.
    *   **Example**: If CEX phase gives a SELL at 50% and Wallet Behavior phase gives a SELL at 40%, the combined confidence should be greater than 50% (e.g., `(0.50 + 0.40) * 0.8 = 0.72`), not just the max.
4.  **Introduce "Medium Confidence" Signals**: Modify the final classification logic to produce `MODERATE_BUY` and `MODERATE_SELL` signals if the final confidence is between 60% and 80%. High-confidence signals should be reserved for >80%.

### B. Expanded DeFi Protocol & Pattern Intelligence

We must classify more DeFi interactions as BUY/SELL based on economic intent.

1.  **Yield Farming & LP-ing as BUY**: In `_classify_defi_protocol`, transactions involving protocols like **Yearn, Convex, Harvest, BadgerDAO, and general "Vault" or "Farm" interactions** should be classified as `DEFI`, which is then mapped to `BUY`. This represents an investment action.
2.  **Comprehensive Liquid Staking**: Expand staking detection beyond the current list. Add **Frax Ether (sfrxETH), StakeWise (osETH), Swell (swETH), and Ankr Staked ETH (aETHc)**. Ensure these are all mapped to `BUY`.
3.  **Bridge Transaction Intelligence (CRITICAL)**: **Stop automatically classifying bridges as "TRANSFER."** Introduce logic to infer direction:
    *   A transaction bridging from a Layer-1 (like Ethereum) to a Layer-2 (like Arbitrum, Optimism) or a new ecosystem is often an **accumulation/investment signal (`BUY`)**.
    *   A transaction bridging from a Layer-2 back to Ethereum, especially into a CEX-tagged wallet, is a **de-risking/selling signal (`SELL`)**.
    *   Your logic should check the `from` and `to` chain context if available, or use address tags as a proxy.

### C. Smarter Heuristics & Contextual Analysis

1.  **USD Value Weighting**: In all classification phases, incorporate the transaction's USD value. High-value transactions (e.g., >$100,000) should receive a confidence boost (e.g., +10-15%). A $5M transaction is more intentional than a $5k one.
2.  **Token-to-Token Swap Heuristics**: In `_analyze_evm_transaction` or a similar method, improve token-to-token swap logic. Instead of defaulting to TRANSFER, use heuristics:
    *   **Market Cap / Popularity**: Swapping from a high-market-cap token (like WETH) to a low-cap token is a speculative `BUY`. The reverse is a `SELL`.
    *   **Token Type**: Swapping from a governance token to a utility token could be a `SELL` of influence.
3.  **Gas Price Intelligence**: In `_apply_behavioral_heuristics`, factor in gas price. A transaction with a very high gas fee (priority fee) indicates urgency. This should boost the confidence of a `BUY` or `SELL` classification.

## 📝 Implementation Guidelines & Constraints

*   **Targeted Files**: Confine your code changes primarily to `utils/classification_final.py` and `config/settings.py`.
*   **Maintain Core Architecture**: Do not change the main function signature `analyze_transaction_comprehensive` or the structure of the `IntelligenceResult` it returns. Enhance the logic *within* the existing pipeline.
*   **Production Quality**: Write clean, efficient, and well-documented Python code. Add comments explaining the "why" behind your new, more nuanced logic.
*   **Provide a Summary of Changes**: At the end, please provide a brief markdown summary of the key enhancements you implemented and where you implemented them.

Execute this plan to build a truly intelligent whale transaction classifier. 