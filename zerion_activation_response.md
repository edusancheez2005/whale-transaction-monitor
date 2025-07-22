# Zerion API Development Key Activation Request

**To:** Abi & Jules (Zerion API Team)  
**From:** [Your Name]  
**Subject:** Use Case Details for Zerion API Development Key Activation  
**Key:** `zk_dev_aaf4e06cb16a4d5caa46bb3d421b7098`

---

## 📋 **Project Overview**

We are building a **Professional Whale Transaction Monitoring System** that tracks large cryptocurrency transactions across multiple blockchains (Ethereum, Polygon, Solana) to identify whale trading patterns and provide real-time market intelligence.

## 🎯 **Specific Use Case for Zerion API**

### **Primary Integration: Portfolio Enrichment**
- **Endpoint:** `/v1/wallets/[ADDRESS]/portfolio` 
- **Purpose:** Calculate whale scores and classification based on total portfolio value
- **Classification Tiers:**
  - Mega Whale: $50M+ portfolio
  - Large Whale: $10M+ portfolio  
  - Whale: $1M+ portfolio
  - Small Whale: $100K+ portfolio
  - Retail Investor: <$100K

### **Secondary Integration: Transaction Analysis**
- **Endpoint:** `/v1/wallets/[ADDRESS]/transactions`
- **Purpose:** Analyze transaction patterns to detect:
  - Whale → DEX transactions (SELL signals)
  - DEX → Whale transactions (BUY signals)
  - Filter spam transactions using `filter[trash]=only_non_trash`

### **Tertiary Integration: Position Analysis**
- **Endpoint:** `/v1/wallets/[ADDRESS]/positions/`
- **Purpose:** Detailed portfolio composition analysis for whale behavior patterns

## 📊 **Expected Usage Patterns**

- **Volume:** ~2,000-3,000 API calls per day initially
- **Growth:** Scaling to production key (5K+ calls/day) within 30 days
- **Rate Limiting:** Respecting 120 req/min limit with proper queuing
- **Target Addresses:** Focusing on known whale addresses and large transaction participants

## 🏗️ **Technical Implementation**

### **Current Architecture:**
```python
# Professional whale classification using Zerion portfolio data
def calculate_whale_score(portfolio_value: float) -> tuple[float, str]:
    if portfolio_value >= 50_000_000:
        return 10.0, "Mega Whale"
    elif portfolio_value >= 10_000_000:
        return 8.5, "Large Whale"
    elif portfolio_value >= 1_000_000:
        return 7.0, "Whale"
    elif portfolio_value >= 100_000:
        return 5.0, "Small Whale"
    else:
        return min(5.0, (portfolio_value / 100_000) * 5.0), "Retail Investor"
```

### **Integration Points:**
- Enhanced transaction classification using portfolio context
- Real-time whale activity monitoring across multiple chains
- Market impact analysis based on whale portfolio movements

## 🎉 **Value Proposition**

This integration will help us provide:
- **Accurate whale detection** beyond simple transaction amounts
- **Portfolio-based classification** for better market intelligence
- **Real-time trading signals** based on whale activity patterns
- **Institutional-grade analytics** for crypto market participants

## 🚀 **Future Roadmap**

**Phase 1 (Dev Key):** Portfolio enrichment and basic whale classification  
**Phase 2 (Production Key):** Real-time monitoring with high-volume processing  
**Phase 3 (Enterprise):** Advanced analytics with custom whale behavior models

## 📞 **Communication Preference**

We would welcome a **dedicated Slack channel** for real-time communication during development and implementation phases.

---

**Thank you for enabling professional whale transaction monitoring with Zerion's comprehensive onchain data! Looking forward to building something amazing together.**

---

## 🔧 **Technical Setup Ready**

Our system is already implemented with professional standards:
- ✅ Proper Basic Auth implementation
- ✅ Rate limiting compliance (120 req/min)
- ✅ Error handling and retry logic
- ✅ Centralized logging and monitoring
- ✅ Production-ready architecture

**Ready to integrate immediately upon key activation!** 