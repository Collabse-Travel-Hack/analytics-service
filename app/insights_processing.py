from clickhouse_driver import Client
from collections import defaultdict


clickhouse_client = Client(host='158.160.14.223', port=9000)

def aggregate_data():
    kafka_seen_media_events = clickhouse_client.execute("SELECT * FROM kafka_seen_media_events")
    kafka_clicked_events = clickhouse_client.execute("SELECT * FROM kafka_clicked_events")
    kafka_bookmarked_events = clickhouse_client.execute("SELECT * FROM kafka_bookmarked_events")
    kafka_bought_events = clickhouse_client.execute("SELECT * FROM kafka_bought_events")
    kafka_commented_events = clickhouse_client.execute("SELECT * FROM kafka_commented_events")
    kafka_media_uploaded_events = clickhouse_client.execute("SELECT * FROM kafka_media_uploaded_events")

    result = defaultdict(lambda: {
        "seen_media": {"count": 0, "events": []},
        "clicked": {"count": 0, "events": []},
        "bookmarked": {"count": 0, "events": []},
        "bought": {"count": 0, "events": []},
        "commented": {"count": 0, "events": []},
        "media_uploaded": {"count": 0, "events": []},
        "total_events": 0
    })

    for seen_media in kafka_seen_media_events:
        timestamp, account_id, product_id, media_type, media_id, media_name = seen_media
        result[product_id]["seen_media"]["events"].append({
            "timestamp": timestamp,
            "account_id": account_id,
            "product_id": product_id,
            "media_type": media_type,
            "media_id": media_id,
            "media_name": media_name
        })
        result[product_id]["seen_media"]["count"] += 1
        result[product_id]["total_events"] += 1

    for click in kafka_clicked_events:
        timestamp, account_id, product_id, event_type = click
        result[product_id]["clicked"]["events"].append({
            "timestamp": timestamp,
            "account_id": account_id,
            "event_type": event_type
        })
        result[product_id]["clicked"]["count"] += 1
        result[product_id]["total_events"] += 1

    for bookmark in kafka_bookmarked_events:
        timestamp, account_id, product_id = bookmark
        result[product_id]["bookmarked"]["events"].append({
            "timestamp": timestamp,
            "account_id": account_id
        })
        result[product_id]["bookmarked"]["count"] += 1
        result[product_id]["total_events"] += 1

    for buy in kafka_bought_events:
        timestamp, account_id, product_id, price = buy
        result[product_id]["bought"]["events"].append({
            "timestamp": timestamp,
            "account_id": account_id,
            "price": price
        })
        result[product_id]["bought"]["count"] += 1
        result[product_id]["total_events"] += 1

    for comment in kafka_commented_events:
        timestamp, account_id, product_id, text = comment
        result[product_id]["commented"]["events"].append({
            "timestamp": timestamp,
            "account_id": account_id,
            "text": text
        })
        result[product_id]["commented"]["count"] += 1
        result[product_id]["total_events"] += 1

    for media_uploaded in kafka_media_uploaded_events:
        timestamp, account_id, product_id, media_type, media_id, media_name = media_uploaded
        result[product_id]["media_uploaded"]["events"].append({
            "timestamp": timestamp,
            "account_id": account_id,
            "product_id": product_id,
            "media_type": media_type,
            "media_id": media_id,
            "media_name": media_name
        })
        result[product_id]["media_uploaded"]["count"] += 1
        result[product_id]["total_events"] += 1

    return dict(result)