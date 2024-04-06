from clickhouse_driver import Client


clickhouse_client = Client(host='158.160.14.223', port=9000)

def aggregate_data():
    # Retrieve data from ClickHouse for each event type
    bookmark_events = clickhouse_client.execute("SELECT * FROM bookmark_events")
    bought_events = clickhouse_client.execute("SELECT * FROM bought_events")
    clicked_events = clickhouse_client.execute("SELECT * FROM clicked_events")
    commented_events = clickhouse_client.execute("SELECT * FROM commented_events")
    media_uploaded_events = clickhouse_client.execute("SELECT * FROM media_uploaded_events")
    media_seen_events = clickhouse_client.execute("SELECT * FROM media_seen_events")

    # Aggregate the data as per your requirements
    insights = {
        "total_bookmarks": len(bookmark_events),
        "total_purchases": len(bought_events),
        "total_clicks": len(clicked_events),
        "total_comments": len(commented_events),
        "total_media_uploads": len(media_uploaded_events),
        "total_media_views": len(media_seen_events),
        # Add more aggregations as needed
    }

    return insights