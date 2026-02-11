import frappe
import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import os
import subprocess

@frappe.whitelist()
def force_sync(modules):
    if isinstance(modules, str):
        modules = json.loads(modules)
    results = {}
    for module in modules:
        try:
            if module == "items":
                results["items"] = sync_items() 
            elif module == "customers":
                results["customers"] = sync_customers() 
            elif module == "item_prices":
                results["item_prices"] = sync_item_prices() 
            elif module == "price_lists":
                results["price_lists"] = sync_price_lists()  
            elif module == "sales_invoices":
                results["sales_invoices"] = push_pending_invoices() 
            else:
                results[module] = "Unknown module"
        except Exception as e:
         results[module] = f"Failed: {str(e)}"

    return {"success": True, "details": results}
import frappe
import requests

@frappe.whitelist()
def sync_items():
    def debug(msg):
        print(msg)
        frappe.log_error(message=msg, title="Sync Debug")

    debug("🔹 Starting sync_items()")

    try:
        settings = frappe.get_single("Sync Settings")
        cloud_url = settings.cloud_site_url.rstrip("/")
        endpoint_base = f"{cloud_url}/api/method/saas_api.www.api.get_products_saas"

        all_products = []
        page = 1
        limit = 1000

        # Fetch all pages safely
        while True:
            endpoint = f"{endpoint_base}?limit={limit}&page={page}"
            try:
                response = requests.get(endpoint, timeout=120)
                response.raise_for_status()
                data = response.json().get("message", {})
            except Exception as e:
                debug(f"❌ Failed to fetch page {page}: {str(e)}")
                break  # stop fetching, don’t throw

            products = data.get("products", [])
            if not products:
                debug(f"🔹 No more products on page {page}")
                break

            all_products.extend(products)
            debug(f"🔹 Fetched {len(products)} products from page {page}, total so far: {len(all_products)}")

            pagination = data.get("pagination", {})
            if not pagination.get("has_next_page") or not pagination.get("next_page"):
                break
            page += 1

        if not all_products:
            debug("🔹 No products found")
            return "No products found"

        inserted = 0
        updated = 0
        batch_size = 100

        for i, p in enumerate(all_products, start=1):
            item_code = (p.get("itemcode") or "").strip()
            if not item_code:
                debug(f"❌ Skipping item with no code: {p}")
                continue

            item_name = p.get("itemname") or item_code
            group_name = p.get("groupname") or "All Item Groups"
            stock_uom = p.get("uom", {}).get("stock_uom") or "Nos"

            # Ensure group exists
            if not frappe.db.exists("Item Group", group_name):
                frappe.get_doc({
                    "doctype": "Item Group",
                    "item_group_name": group_name,
                    "parent_item_group": "All Item Groups",
                    "is_group": 0
                }).insert(ignore_permissions=True)
                debug(f"🛠 Created Item Group: {group_name}")

            # Ensure UOM exists
            if not frappe.db.exists("UOM", stock_uom):
                frappe.get_doc({"doctype": "UOM", "uom_name": stock_uom}).insert(ignore_permissions=True)
                debug(f"🛠 Created UOM: {stock_uom}")

            # Upsert item
            # Upsert item directly with SQL
            if frappe.db.exists("Item", item_code):
                frappe.db.sql("""
                    UPDATE `tabItem`
                    SET item_name=%s,
                        item_group=%s,
                        stock_uom=%s,
                        is_stock_item=%s,
                        is_sales_item=%s,
                        description=%s
                    WHERE item_code=%s
                """, (
                    item_name,
                    group_name,
                    stock_uom,
                    p.get("maintainstock", 1),
                    p.get("is_sales_item", 1),
                    p.get("description", ""),
                    item_code
                ))
                updated += 1
                debug(f"✏️ Updated Item: {item_code}")
            else:
                frappe.db.sql("""
                    INSERT INTO `tabItem`
                    (item_code, item_name, item_group, stock_uom, is_stock_item, is_sales_item, description)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                """, (
                    item_code,
                    item_name,
                    group_name,
                    stock_uom,
                    p.get("maintainstock", 1),
                    p.get("is_sales_item", 1),
                    p.get("description", "")
                ))
                inserted += 1
                debug(f"✅ Inserted Item: {item_code}")

            # Prices
            for price in p.get("prices", []):
                price_list = price.get("priceName") or "Standard Selling"
                uom = price.get("uom") or "Nos"
                selling = 1 if price.get("type") == "selling" else 0
                buying = 1 if price.get("type") == "buying" else 0

                existing_price = frappe.db.exists("Item Price", {
                    "item_code": item_code,
                    "price_list": price_list,
                    "uom": uom,
                    "selling": selling,
                    "buying": buying
                })

                if existing_price:
                    price_doc = frappe.get_doc("Item Price", existing_price)
                    price_doc.price_list_rate = price.get("price")
                    price_doc.save(ignore_permissions=True)
                    debug(f"✏️ Updated Price: {item_code} / {price_list}")
                else:
                    frappe.get_doc({
                        "doctype": "Item Price",
                        "item_code": item_code,
                        "price_list": price_list,
                        "price_list_rate": price.get("price"),
                        "selling": selling,
                        "buying": buying,
                        "currency": "USD"
                    }).insert(ignore_permissions=True)
                    debug(f"✅ Inserted Price: {item_code} / {price_list}")

            # Commit every batch
            if i % batch_size == 0:
                frappe.db.commit()
                debug(f"💾 Committed batch of {batch_size} items")

        # Final commit
        frappe.db.commit()
        debug(f"🎉 Sync complete. Inserted {inserted}, Updated {updated} items.")
        return f"Inserted {inserted}, Updated {updated} items."

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
        cloud_url = settings.cloud_site_url.rstrip("/")
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
            raise Exception(f"Failed to fetch customers: {response.text}")

        customers = response.json().get("data", [])
        debug(f"🔹 Fetched {len(customers)} customers")

        if not customers:
            return "No new customers to sync"

        # Try resolving defaults once (cheap + safe)
        default_warehouse = None
        default_cost_center = None

        if frappe.db.has_column("Customer", "custom_warehouse"):
            default_warehouse = frappe.db.get_value(
                "Warehouse", {"is_group": 0}, "name"
            )

        if frappe.db.has_column("Customer", "custom_cost_center"):
            default_cost_center = frappe.db.get_value(
                "Cost Center", {"is_group": 0}, "name"
            )

        for cust in customers:
            name = cust.get("name")
            customer_name = cust.get("customer_name") or name
            customer_group = cust.get("customer_group") or "All Customer Groups"
            territory = cust.get("territory") or "All Territories"
            customer_type = cust.get("customer_type") or "Company"

            print(f"🔹 Processing customer: {cust}")

            if not name:
                debug(f"❌ Skipped customer, missing name: {cust}")
                continue

            ensure_customer_group(customer_group)
            ensure_territory(territory)

            # Guard: mandatory custom fields but no defaults
            if (
                frappe.db.has_column("Customer", "custom_warehouse")
                and frappe.get_meta("Customer").get_field("custom_warehouse").reqd
                and not default_warehouse
            ):
                debug(f"⚠️ Skipping {name}: missing default warehouse")
                continue

            if (
                frappe.db.has_column("Customer", "custom_cost_center")
                and frappe.get_meta("Customer").get_field("custom_cost_center").reqd
                and not default_cost_center
            ):
                debug(f"⚠️ Skipping {name}: missing default cost center")
                continue

            if frappe.db.exists("Customer", name):
                doc = frappe.get_doc("Customer", name)
                doc.customer_name = customer_name
                doc.customer_group = customer_group
                doc.territory = territory
                doc.customer_type = customer_type

                if hasattr(doc, "custom_warehouse") and default_warehouse:
                    doc.custom_warehouse = doc.custom_warehouse or default_warehouse

                if hasattr(doc, "custom_cost_center") and default_cost_center:
                    doc.custom_cost_center = doc.custom_cost_center or default_cost_center

                doc.save(ignore_permissions=True)
                print(f"🔁 Updated customer: {name}")

            else:
                payload = {
                    "doctype": "Customer",
                    "name": name,
                    "customer_name": customer_name,
                    "customer_group": customer_group,
                    "territory": territory,
                    "customer_type": customer_type
                }

                if default_warehouse:
                    payload["custom_warehouse"] = default_warehouse
                if default_cost_center:
                    payload["custom_cost_center"] = default_cost_center

                frappe.get_doc(payload).insert(ignore_permissions=True)
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




@frappe.whitelist()
def push_pending_invoices():
    """
    Push pending Sales Invoices to remote Frappe site.
    On success:
      - Marks local Sales Invoice as synced
      - Stores remote Sales Invoice number in custom_reference
      - Marks outbox as Synced
    Safe for cron.
    """

    print("🚀 Starting push_pending_invoices")

    settings = frappe.get_single("Sync Settings")

    cloud_url = settings.cloud_site_url.rstrip("/")
    remote_company = settings.remote_company
    processed = 0
    pending = frappe.get_all(
        "Sales Invoice Outbox",
        filters={"status": "Pending"},
        fields=["name", "sales_invoice", "retry_count"]
    )
    print(f"🔹 Found {len(pending)} pending invoices")
    for row in pending:
        try:
            invoice = frappe.get_doc("Sales Invoice", row.sales_invoice)

            # 🔒 Skip already-synced invoices
            if invoice.get("custom_synced"):
                print(f"⏭ {invoice.name} already synced, skipping")
                continue
            print(f"📦 Preparing {invoice.name}")
            # Build Sales Invoice payload
            si_doc = {
                "doctype": "Sales Invoice",
                "company": remote_company,
                "customer": invoice.customer,
                "posting_date": str(invoice.posting_date),
                "posting_time": str(invoice.posting_time),
                "due_date": str(invoice.due_date),
                "currency": invoice.currency or "USD",
                "conversion_rate": invoice.conversion_rate or 1,
                "update_stock": invoice.get("update_stock", 1),
                "set_warehouse": invoice.set_warehouse,
                "cost_center": invoice.cost_center,
                "taxes_and_charges": invoice.taxes_and_charges,
                "payments": invoice.get("payments", []),
                "reference_number":row.sales_invoice,
                "items": [
                    {
                        "item_code": d.item_code,
                        "item_name": d.item_name,
                        "qty": d.qty,
                        "rate": d.rate,
                        "warehouse": d.warehouse,
                        "cost_center": d.cost_center,
                        "income_account": d.income_account
                    }
                    for d in invoice.items
                ]
            }
            endpoint = f"{cloud_url}/api/method/saas_api.www.api.cloud_invoice"
            response = requests.post(
                endpoint,
                json=si_doc,
                timeout=60
            )
            resp_json = response.json()
            remote_si = resp_json.get("message", {}).get("data", {}).get("name")
            print(f"remote reference received--------------{remote_si}")

            outbox = frappe.get_doc("Sales Invoice Outbox", row.name)
            outbox.last_attempt = datetime.now()
            outbox.retry_count += 1

            # ✅ SUCCESS
            if response.status_code in (200, 201):
                resp_json = response.json()
                remote_si = resp_json.get("message", {}).get("data", {}).get("name")

                # Update local Sales Invoice (NO save)
                invoice.db_set("custom_synced", 1)
                invoice.db_set("custom_cloud_reference", remote_si)

                outbox.status = "Synced"
                outbox.save(ignore_permissions=True)

                print(f"✅ {invoice.name} → {remote_si}")

            # ❌ FAILURE
            else:
                outbox.status = "Failed"
                outbox.error_message = f"{response.status_code} - {response.text}"
                outbox.save(ignore_permissions=True)

                frappe.log_error(
                    title="Sales Invoice Sync Failed",
                    message=f"{invoice.name}: {response.status_code} - {response.text}"
                )

                print(f"❌ Failed {invoice.name}")

            processed += 1
        except Exception as e:
            frappe.log_error(
                title="Sales Invoice Sync Exception",
                message=f"{row.sales_invoice}: {str(e)}"
            )
            print(f"🔥 Exception on {row.sales_invoice}: {e}")
    print(f"🎉 Finished. Processed {processed} invoices")
    return f"Processed {processed} invoices"


@frappe.whitelist()
def sync_from_remote():
    """
    Enqueues a background job to sync all master data from cloud.
    """
    frappe.enqueue(
        "sync_master.sync_master.api.call_all_pulls",
        queue="short",
        timeout=600
    )
    frappe.publish_realtime(
        "msg",
        "Cloud sync job has been enqueued!",
        user="Administrator"
    )
    return "Cloud sync job enqueued successfully"


def call_all_pulls():
    results = {}

    for name, fn in {
        "items": sync_items
        # "item_prices": sync_item_prices,
        # "customers": sync_customers,
        # "price_lists": sync_price_lists,
    }.items():
        try:
            results[name] = fn()
        except Exception as e:
            results[name] = f"Failed: {str(e)}"
            frappe.log_error(
                message=frappe.get_traceback(),
                title=f"Cloud Pull Failed: {name}"
            )

    return results

import os
import subprocess
import frappe

@frappe.whitelist()
def setup_cloud_sync_service_for_site():
    """
    Creates a systemd service and timer for cloud sync for the current site.
    The service and timer names include the site name for multi-tenancy support.
    Saves the generated service name in Sync Settings.
    """
    # Get site name from Sync Settings
    settings = frappe.get_single("Sync Settings")
    site_name = getattr(settings, "site_name", None)
    if not site_name:
        frappe.throw("Site name not set in Sync Settings!")

    # Paths
    bench_path = "/home/frappe/frappe-bench"
    python_bench = "/home/frappe/frappe-env/bin/bench"
    service_name = f"cloud-sync-{site_name}.service"
    timer_name = f"cloud-sync-{site_name}.timer"
    service_path = os.path.join(bench_path, service_name)
    timer_path = os.path.join(bench_path, timer_name)

    # Service content
    service_content = f"""[Unit]
Description=Frappe Cloud Sync Job for {site_name}
After=network.target

[Service]
Type=oneshot
User=frappe
WorkingDirectory={bench_path}
ExecStart={python_bench} --site {site_name} execute sync_master.sync_master.api.sync_from_remote
"""

    # Timer content
    timer_content = f"""[Unit]
Description=Run Frappe Cloud Sync for {site_name} every 5 minutes

[Timer]
OnBootSec=1min
OnUnitActiveSec=1min
Persistent=true

[Install]
WantedBy=timers.target
"""

    # Write service + timer files in user space
    with open(service_path, "w") as f:
        f.write(service_content)

    with open(timer_path, "w") as f:
        f.write(timer_content)

    # Save generated service name in settings
    settings.db_set("service_name_generated", service_name)

    # Reload systemd (manual step still required) if you want, just print instructions
    frappe.msgprint(
        f"✅ Cloud sync service & timer files created for site: {site_name}\n\n"
        f"Files:\n{service_path}\n{timer_path}\n\n"
        f"Now you can manually move them to /etc/systemd/system/ and enable the timer:\n"
        f"sudo mv {service_path} /etc/systemd/system/\n"
        f"sudo mv {timer_path} /etc/systemd/system/\n"
        f"sudo systemctl daemon-reload\n"
        f"sudo systemctl enable --now {timer_name}\n"
    )
