import asyncio
import websockets
import json
import os
import sys

# --- 1. ENVIRONMENT SETUP ---
BENCH_PATH = "/home/munyaradzi/Documents/frappe-bench"
SITES_PATH = os.path.join(BENCH_PATH, "sites")
sys.path.append(os.path.join(BENCH_PATH, "apps", "frappe"))
sys.path.append(os.path.join(BENCH_PATH, "apps", "erpnext"))
sys.path.append(os.path.join(BENCH_PATH, "apps", "saas_api"))
os.environ["FRAPPE_SITES_PATH"] = SITES_PATH
os.chdir(SITES_PATH)

import frappe

def get_sync_config():
    """Fetch cloud URI, cloud site, and local site from Sync Settings"""
    frappe.init(site="labmaster.local")  
    frappe.connect()
    try:
        settings = frappe.get_doc("Sync Settings", "Sync Settings")
        cloud_site = settings.cloud_site    
        local_site = settings.local_site_name 
        cloud_domain = settings.cloud_site_url 
    
    

        return {
            "cloud_site": cloud_site,
            "local_site": local_site,
            "cloud_site_url": cloud_domain
        }
    finally:
        frappe.destroy()

# # --- 2. CONFIG ---
# CLOUD_SITE = "pay.havano.cloud"
# LOCAL_SITE = "labmaster.local"
# CLOUD_WS_URI = "ws://62.171.186.89:8765"

config = get_sync_config()
CLOUD_SITE = config["cloud_site"]
LOCAL_SITE = config["local_site"]
CLOUD_WS_URI = config["cloud_site_url"]

print(f"Config - Cloud Site: {CLOUD_SITE}, Local Site: {LOCAL_SITE}, Cloud WS URI: {CLOUD_WS_URI}")

DEFAULT_MODIFIED = "1970-01-01T00:00:00"

def get_local_sync_time(site):
    """Safely get the last sync time from the local DB."""
    frappe.init(site=site)
    frappe.connect()
    try:
        val = frappe.db.get_value("Sync Settings", "Sync Settings", "quotation_last_pulled")
        return val or DEFAULT_MODIFIED
    finally:
        frappe.destroy()

def save_and_update_sync(site, quotations):
    if not quotations:
        return None
    
    frappe.init(site=site)
    frappe.connect()
    frappe.set_user("Administrator")
    
    try:
        for q in quotations:
            status_val = {"Draft": 0, "Submitted": 1, "Cancelled": 2}.get(q["status"], 0)
            if frappe.db.exists("Quotation", q["name"]):
                frappe.db.set_value("Quotation", q["name"], {
                    "transaction_date": q["transaction_date"],
                    "valid_till": q["valid_till"],
                    "grand_total": q["grand_total"],
                    "docstatus": status_val,
                    "company": q["company"]
                }, update_modified=True)
                
                frappe.db.delete("Quotation Item", {"parent": q["name"]})
            else:
                new_q = frappe.new_doc("Quotation")
                new_q.name = q["name"]
                new_q.update({
                    "transaction_date": q["transaction_date"],
                    "valid_till": q["valid_till"],
                    "grand_total": q["grand_total"],
                    "docstatus": status_val,
                    "company": q["company"]
                })
                new_q.db_insert() 

            for i, item in enumerate(q.get("items", [])):
                child = frappe.new_doc("Quotation Item")
                child.update({
                    "parent": q["name"],
                    "parentfield": "items",
                    "parenttype": "Quotation",
                    "idx": i + 1,
                    "item_code": item.get("item_code"),
                    "item_name": item.get("item_name"),
                    "qty": item.get("qty") or 1.0,
                    "rate": item.get("rate") or 0.0,
                    "amount": item.get("amount") or 0.0,
                    "uom": item.get("uom") or "Nos",
                    "conversion_factor": item.get("conversion_factor") or 1.0,
                })
                child.db_insert()

        last_modified = max(q["modified"] for q in quotations)
        if frappe.db.exists("Sync Settings", "Sync Settings"):
            print("last modified: ", last_modified)
            frappe.db.set_value("Sync Settings", "Sync Settings", "quotation_last_pulled", last_modified)


        frappe.db.commit()
        return last_modified
    except Exception:
        print("!!! DB Sync Error:")
        print(frappe.get_traceback())
        frappe.db.rollback()
        return None
    finally:
        frappe.destroy()

async def pull_batches(websocket, current_modified):
    while True:
        try:
            request = {
                "action": "pull_quotations",
                "site_id": CLOUD_SITE,
                "modified_since": current_modified,
                "limit": 5
            }
            await websocket.send(json.dumps(request))
            
            response = await websocket.recv()
            data = json.loads(response)

            batch = data.get("quotations", [])
            print(f"Received batch of {len(batch)} quotations modified since {current_modified}")

            if not batch:
                print("No new data. Waiting 10s...")
                await asyncio.sleep(10)
                continue

     
            new_timestamp = await asyncio.to_thread(save_and_update_sync, LOCAL_SITE, batch)
            
            if new_timestamp:
                current_modified = new_timestamp
                print(f"Synced up to: {current_modified}")
            
            await asyncio.sleep(2)

        except websockets.exceptions.ConnectionClosed:
            break
        except Exception as e:
            print(f"Loop Error: {e}")
            await asyncio.sleep(5)

async def connect():
    while True:
        try:
            start_time = await asyncio.to_thread(get_local_sync_time, LOCAL_SITE)
            print(f"Starting sync from: {start_time}")

            async with websockets.connect(CLOUD_WS_URI) as websocket:
                await websocket.send(json.dumps({"site_id": CLOUD_SITE}))
                await pull_batches(websocket, start_time)
        except Exception as e:
            print(f"Connection failed: {e}. Retrying...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(connect())
