# Rumee Dashboard

Live dashboard for Rumee Jewellery sales across Flipkart and Meesho.

**Live URL:** https://rumeein.github.io/rumee-dashboard/

---

## Weekly Update Flow

1. Download fresh export files from Flipkart and Meesho seller portals
2. Drop them into the `new_data/` folder
3. Run: `python process.py`
4. Commit and push:
   ```
   git add rumee_db_v1.csv index.html
   git commit -m "Data update: YYYY-MM-DD"
   git push origin main
   ```

The dashboard auto-fetches `rumee_db_v1.csv` from GitHub on every load.

---

## File Types Supported

| File | Source | What it updates |
|------|--------|-----------------|
| `combined_orders_data.csv` | Meesho > Orders > Download | Monthly GMV, orders, returns |
| `Return_*.csv` | Meesho > Returns > Download | Return reasons, SKU return rates |
| `combined_order_payments_data.xlsx` | Meesho > Payments | Monthly settlement |
| `combined_ads_cost_data.xlsx` | Meesho > Ads | Monthly ad spend |
| `Catelog details.xlsx` | Meesho > My Products | Stock levels |
| `combined_flipkart_Payment_data.xlsx` | Flipkart > Payments | FK monthly + SKU data |
| `combined_flipkart_ads_data.xlsx` | Flipkart > Advertising | FK ad spend |
| `Rumme_Processed_Views.csv` | Flipkart > Listing Performance | FK SKU views + CTR |
| `Rumme_Keywords_Processed.csv` | Flipkart > Keyword Performance | Keywords (future use) |

## Deduplication

`process.py` tracks the last processed date per file type in `rumee_db_v1.csv` under the `config` table. Rows already processed are automatically skipped.

---

## Database Format

`rumee_db_v1.csv` is a multi-table CSV. Each table starts with `__table__` header row:

```
__table__,key,value
config,last_updated,2026-05-23
...
__table__,month,label,gmv,settlement,orders,returns,ad_spend
fk_monthly,2026-01,Jan,142009,74527,673,344,214
...
```
