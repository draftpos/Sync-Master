import frappe
import requests
from requests.auth import HTTPBasicAuth
import json


@frappe.whitelist()
def force_sync(modules):
    """
    modules: list of strings ['items', 'customers', 'price_lists', 'sales_invoices']
    Triggers the corresponding sync function for each selected module.
    """
    # Handle case if modules comes as JSON string
    if isinstance(modules, str):
        modules = json.loads(modules)

    results = {}

    for module in modules:
        try:
            if module == "items":
                results["items"] = sync_items()  # your item fetch function
            elif module == "customers":
                results["customers"] = sync_customers()  # to implement
            elif module == "item_prices":
                results["item_prices"] = sync_item_prices()  # to implement
            elif module == "price_lists":
                results["price_lists"] = sync_price_lists()  # to implement
            elif module == "sales_invoices":
                results["sales_invoices"] = push_pending_invoices()  # to implement
            else:
                results[module] = "Unknown module"
        except Exception as e:
            results[module] = f"Failed: {str(e)}"

    return {"success": True, "details": results}

def sync_items():
    # Helper to print + log error
    def debug(msg):
        print(msg)
        frappe.log_error(message=msg, title="Sync Debug")

    debug("🔹 Starting sync_items()")

    try:
        settings = frappe.get_single("Sync Settings")
        cloud_url = settings.cloud_site_url
        api_key = settings.api_key
        api_secret = settings.api_secret
        last_synced_at = settings.last_item_sync or "1970-01-01 00:00:00"

        debug(f"🔹 Fetching items modified after: {last_synced_at}")
        endpoint = f"{cloud_url}/api/resource/Item"
        params = {
            "filters": f'[["modified", ">", "{last_synced_at}"]]',
            "fields": '["name","item_code","item_name","description","item_group","stock_uom","modified"]',
            "order_by": "modified asc,name asc",
            "limit_page_length": 500
        }

        response = requests.get(
            endpoint,
            params=params,
            auth=HTTPBasicAuth(api_key, api_secret),
            timeout=30
        )

        debug(f"🔹 HTTP GET to {endpoint} returned status {response.status_code}")

        if response.status_code != 200:
            msg = f"❌ Failed to fetch items: {response.text}"
            debug(msg)
            frappe.throw(msg)

        data = response.json()
        items = data.get("data", [])

        debug(f"🔹 Fetched {len(items)} items from cloud")

        if not items:
            debug("🔹 No new items to sync")
            return "No new items to sync"
        for item in items:
            item_code = item.get("item_code") or item.get("name")
            item_name = item.get("item_name") or item_code
            item_group = item.get("item_group")
            print(f"🔹 Processing item ----------------- {item}")
            if not item_code:
                msg = f"❌ Skipped item, missing identifier: {item}"
                print(msg)
                frappe.log_error(message=msg, title="Sync Skipped Item")
                continue
            ensure_item_group(item_group)

            doc = frappe.get_doc({
                "doctype": "Item",
                "item_code": item_code,
                "item_name": item_name,
                "description": item.get("description"),
                "item_group": item_group,
                "stock_uom": item.get("stock_uom"),
            })

            doc.insert(ignore_permissions=True, ignore_if_duplicate=True)
            print(f"✅ Upserted item: {item_code}")
        last_modified = items[-1].get("modified") or last_synced_at
        settings.db_set("last_item_sync", last_modified)
        debug(f"🔹 Updated last_item_sync to {last_modified}")
        debug(f"🎉 Synced {len(items)} items successfully")
        return f"Synced {len(items)} items"
    except Exception as e:
        debug(f"❌ Exception in sync_items: {str(e)}")
        raise
def ensure_item_group(item_group):
    if not item_group:
        return

    if frappe.db.exists("Item Group", item_group):
        return

    print(f"🛠 Creating missing Item Group: {item_group}")

    frappe.get_doc({
        "doctype": "Item Group",
        "item_group_name": item_group,
        "parent_item_group": "All Item Groups",
        "is_group": 0
    }).insert(ignore_permissions=True)

def sync_customers():
    def debug(msg):
        print(msg)
        frappe.log_error(message=msg, title="Customer Sync Debug")

    debug("🔹 Starting sync_customers()")

    try:
        settings = frappe.get_single("Sync Settings")
        cloud_url = settings.cloud_site_url
        api_key = settings.api_key
        api_secret = settings.api_secret
        last_synced_at = settings.customer_last_sync or "1970-01-01 00:00:00"

        debug(f"🔹 Fetching customers modified after: {last_synced_at}")

        endpoint = f"{cloud_url}/api/resource/Customer"
        params = {
            "filters": f'[["modified", ">", "{last_synced_at}"]]',
            "fields": '["name","customer_name","customer_group","territory","customer_type","modified"]',
            "order_by": "modified asc,name asc",
            "limit_page_length": 500
        }

        response = requests.get(
            endpoint,
            params=params,
            auth=HTTPBasicAuth(api_key, api_secret),
            timeout=30
        )

        debug(f"🔹 HTTP GET returned {response.status_code}")

        if response.status_code != 200:
            msg = f"❌ Failed to fetch customers: {response.text}"
            debug(msg)
            frappe.throw(msg)

        customers = response.json().get("data", [])
        debug(f"🔹 Fetched {len(customers)} customers")

        if not customers:
            return "No new customers to sync"

        for cust in customers:
            name = cust.get("name")
            customer_name = cust.get("customer_name") or name
            customer_group = cust.get("customer_group") or "All Customer Groups"
            territory = cust.get("territory") or "All Territories"
            customer_type = cust.get("customer_type") or "Company"

            print(f"🔹 Processing customer: {cust}")

            if not name:
                msg = f"❌ Skipped customer, missing name: {cust}"
                print(msg)
                frappe.log_error(message=msg, title="Customer Sync Skipped")
                continue

            ensure_customer_group(customer_group)
            ensure_territory(territory)

            if frappe.db.exists("Customer", name):
                doc = frappe.get_doc("Customer", name)
                doc.customer_name = customer_name
                doc.customer_group = customer_group
                doc.territory = territory
                doc.customer_type = customer_type
                doc.save(ignore_permissions=True)
                print(f"🔁 Updated customer: {name}")
            else:
                frappe.get_doc({
                    "doctype": "Customer",
                    "name": name,
                    "customer_name": customer_name,
                    "customer_group": customer_group,
                    "territory": territory,
                    "customer_type": customer_type
                }).insert(ignore_permissions=True)

                print(f"✅ Created customer: {name}")

        last_modified = customers[-1].get("modified") or last_synced_at
        settings.db_set("customer_last_sync", last_modified)

        debug(f"🎉 Customer sync complete, last sync = {last_modified}")
        return f"Synced {len(customers)} customers"

    except Exception as e:
        debug(f"❌ Exception in sync_customers: {str(e)}")
        raise

def ensure_customer_group(group):
    if not group:
        return
    if frappe.db.exists("Customer Group", group):
        return

    print(f"🛠 Creating Customer Group: {group}")
    frappe.get_doc({
        "doctype": "Customer Group",
        "customer_group_name": group,
        "parent_customer_group": "All Customer Groups",
        "is_group": 0
    }).insert(ignore_permissions=True)


def ensure_territory(territory):
    if not territory:
        return
    if frappe.db.exists("Territory", territory):
        return

    print(f"🛠 Creating Territory: {territory}")
    frappe.get_doc({
        "doctype": "Territory",
        "territory_name": territory,
        "parent_territory": "All Territories",
        "is_group": 0
    }).insert(ignore_permissions=True)

def sync_item_prices():
    def debug(msg):
        print(msg)
        frappe.log_error(message=msg, title="Item Price Sync")

    settings = frappe.get_single("Sync Settings")
    cloud_url = settings.cloud_site_url
    api_key = settings.api_key
    api_secret = settings.api_secret
    last_synced_at = settings.price_last_sync or "1970-01-01 00:00:00"

    endpoint = f"{cloud_url}/api/resource/Item Price"
    params = {
        "filters": f'[["modified", ">", "{last_synced_at}"]]',
        "fields": json.dumps([
            "name", "item_code", "price_list",
            "price_list_rate", "currency",
            "selling", "buying", "modified"
        ]),
        "order_by": "modified asc",
        "limit_page_length": 500
    }

    r = requests.get(
        endpoint,
        params=params,
        auth=HTTPBasicAuth(api_key, api_secret),
        timeout=30
    )

    if r.status_code != 200:
        debug(f"❌ Failed Item Price fetch: {r.text}")
        frappe.throw("Item Price sync failed")

    prices = r.json().get("data", [])
    debug(f"🔹 Fetched {len(prices)} item prices")

    for p in prices:
        ensure_price_list(p["price_list"])

        existing = frappe.db.exists(
            "Item Price",
            {
                "item_code": p["item_code"],
                "price_list": p["price_list"],
                "selling": p["selling"],
                "buying": p["buying"]
            }
        )

        if existing:
            doc = frappe.get_doc("Item Price", existing)
            doc.price_list_rate = p["price_list_rate"]
            doc.save(ignore_permissions=True)
            print(f"🔁 Updated price for {p['item_code']} @ {p['price_list']}")
        else:
            frappe.get_doc({
                "doctype": "Item Price",
                "item_code": p["item_code"],
                "price_list": p["price_list"],
                "price_list_rate": p["price_list_rate"],
                "currency": p["currency"],
                "selling": p["selling"],
                "buying": p["buying"]
            }).insert(ignore_permissions=True)
            print(f"✅ Created price for {p['item_code']}")

    if prices:
        settings.db_set("price_last_sync", prices[-1]["modified"])

    return f"Synced {len(prices)} item prices"

def ensure_price_list(price_list):
    if not price_list:
        return

    if frappe.db.exists("Price List", price_list):
        return

    print(f"🛠 Creating missing Price List: {price_list}")

    frappe.get_doc({
        "doctype": "Price List",
        "price_list_name": price_list,
        "enabled": 1,
        "selling": 1
    }).insert(ignore_permissions=True)

def sync_price_lists():
    def debug(msg):
        print(msg)
        frappe.log_error(message=msg, title="Price List Sync")

    debug("🔹 Starting sync_price_lists()")

    try:
        settings = frappe.get_single("Sync Settings")
        cloud_url = settings.cloud_site_url
        api_key = settings.api_key
        api_secret = settings.api_secret
        last_synced_at = settings.price_list_last_sync or "1970-01-01 00:00:00"

        endpoint = f"{cloud_url}/api/resource/Price List"
        params = {
            "filters": f'[["modified", ">", "{last_synced_at}"]]',
            "fields": json.dumps([
                "name",
                "price_list_name",
                "currency",
                "selling",
                "buying",
                "enabled",
                "modified"
            ]),
            "order_by": "modified asc",
            "limit_page_length": 500
        }

        r = requests.get(
            endpoint,
            params=params,
            auth=HTTPBasicAuth(api_key, api_secret),
            timeout=30
        )

        debug(f"🔹 HTTP GET returned {r.status_code}")

        if r.status_code != 200:
            frappe.throw(f"❌ Price List fetch failed: {r.text}")

        price_lists = r.json().get("data", [])
        debug(f"🔹 Fetched {len(price_lists)} price lists")

        for pl in price_lists:
            name = pl["name"]

            if frappe.db.exists("Price List", name):
                doc = frappe.get_doc("Price List", name)
                doc.currency = pl["currency"]
                doc.selling = pl["selling"]
                doc.buying = pl["buying"]
                doc.enabled = pl["enabled"]
                doc.save(ignore_permissions=True)
                print(f"🔁 Updated Price List: {name}")
            else:
                frappe.get_doc({
                    "doctype": "Price List",
                    "price_list_name": name,
                    "currency": pl["currency"],
                    "selling": pl["selling"],
                    "buying": pl["buying"],
                    "enabled": pl["enabled"]
                }).insert(ignore_permissions=True)
                print(f"✅ Created Price List: {name}")

        if price_lists:
            settings.db_set(
                "price_list_last_sync",
                price_lists[-1]["modified"]
            )

        debug("🎉 Price List sync complete")
        return f"Synced {len(price_lists)} price lists"

    except Exception as e:
        debug(f"❌ Exception in sync_price_lists: {str(e)}")
        raise

def create_outbox_record(doc, method):
    """
    Hook for Sales Invoice submit.
    Creates an Outbox record to track syncing to cloud.
    """
    if not frappe.db.exists("Sales Invoice Outbox", {"sales_invoice": doc.name}):
        frappe.get_doc({
            "doctype": "Sales Invoice Outbox",
            "sales_invoice": doc.name,
            "status": "Pending",
            "retry_count": 0
        }).insert(ignore_permissions=True)
        print(f"🔹 Created Outbox record for invoice: {doc.name}")
 
import frappe
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import frappe
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import frappe
from datetime import datetime

@frappe.whitelist()
def push_pending_invoices():
    """
    Push all pending Sales Invoice Outbox records to the remote Frappe site.
    Builds full Sales Invoice doc with defaults to avoid ValidationErrors.
    Cron-friendly with logging.
    """
    print("🚀 Starting push_pending_invoices()")

    # Fetch sync settings
    settings = frappe.get_single("Sync Settings")
    print("🔹 Sync Settings fetched:")
    for field in settings.meta.fields:
        print(f"   {field.label}: {settings.get(field.fieldname)}")

    cloud_url = settings.cloud_site_url
    api_key = settings.api_key
    api_secret = settings.api_secret
    remote_company = settings.remote_company
    processed_count = 0

    # Fetch pending invoices
    pending = frappe.get_all(
        "Sales Invoice Outbox",
        filters={"status": "Pending"},
        fields=["name", "sales_invoice", "retry_count"]
    )
    print(f"🔹 Pending invoices fetched: {len(pending)}")

    for outbox in pending:
        try:
            invoice = frappe.get_doc("Sales Invoice", outbox.sales_invoice)
            print(f"🔹 Preparing invoice {invoice.name} for remote push")

            # Helper to get user/default values
            def get_default(fieldname, fallback=None):
                val = getattr(invoice, fieldname, None)
                if not val:
                    val = fallback
                if not val:
                    frappe.throw(f"{fieldname} missing for invoice {invoice.name}")
                return val

            si_doc = {
                "doctype": "Sales Invoice",
                "company": remote_company,
                "customer": get_default("customer"),
                "posting_date": str(invoice.posting_date),
                "posting_time": str(invoice.posting_time),
                "due_date": str(invoice.due_date),
                "currency": get_default("currency", "USD"),
                "conversion_rate": get_default("conversion_rate", 1.0),
                "update_stock": invoice.get("update_stock", 1),
                "cost_center": invoice.cost_center or "",
                "set_warehouse": invoice.set_warehouse,
                "taxes_and_charges": invoice.get("taxes_and_charges"),
                "payments": invoice.get("payments", []),
                "items": [
                    {
                        "item_code": d.item_code or "",
                        "item_name": d.item_name or "",
                        "qty": d.qty or 0,
                        "rate": d.rate or 0,
                        "warehouse": d.warehouse or "",
                        "cost_center": d.cost_center or "",
                        "income_account": d.income_account or ""
                    } for d in invoice.items
                ]
            }

            # Send to remote using REST API
            import requests
            from requests.auth import HTTPBasicAuth

            endpoint = f"{cloud_url}/api/resource/Sales Invoice"
            print(f"🔹 Sending POST request to {endpoint}")
            response = requests.post(
                endpoint,
                auth=HTTPBasicAuth(api_key, api_secret),
                json=si_doc,
                timeout=60
            )
            print(f"🔹 Response status: {response.status_code}")

            # Update outbox
            invoice_outbox = frappe.get_doc("Sales Invoice Outbox", outbox.name)
            invoice_outbox.last_attempt = datetime.now()
            invoice_outbox.retry_count += 1

            if response.status_code in (200, 201):
                invoice_outbox.status = "Synced"
                invoice_outbox.save(ignore_permissions=True)
                print(f"✅ Synced invoice {invoice.name}")
            else:
                invoice_outbox.status = "Failed"
                invoice_outbox.error_message = f"{response.status_code} - {response.text}"
                invoice_outbox.save(ignore_permissions=True)
                frappe.log_error(
                    message=f"Failed invoice {invoice.name}: {response.status_code} - {response.text}",
                    title="Push Sales Invoice"
                )
                print(f"❌ Failed invoice {invoice.name}: {response.status_code} - {response.text}")

            processed_count += 1

        except Exception as e:
            print(f"❌ Exception while processing {outbox.sales_invoice}: {str(e)}")
            frappe.log_error(
                message=f"Exception for Outbox {outbox.sales_invoice}: {str(e)}",
                title="Push Sales Invoice Exception"
            )
            continue

    print(f"🎉 Processed {processed_count} invoices")
    return f"Processed {processed_count} invoices"
