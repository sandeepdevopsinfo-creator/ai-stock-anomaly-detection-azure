import datetime
import io
import json
import logging
import os
from typing import Any

import azure.functions as func
import pandas as pd
import yfinance as yf
from azure.eventhub import EventData, EventHubProducerClient
from azure.storage.blob import BlobServiceClient
from sklearn.ensemble import IsolationForest


# ---------------------------------------------------------
# Optional Azure Monitor / OpenTelemetry configuration
# ---------------------------------------------------------

try:
    from azure.monitor.opentelemetry import configure_azure_monitor
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode

    application_insights_connection = os.getenv(
        "APPLICATIONINSIGHTS_CONNECTION_STRING"
    )

    if application_insights_connection:
        configure_azure_monitor(
            connection_string=application_insights_connection
        )

    tracer = trace.get_tracer(__name__)

except Exception as telemetry_error:
    logging.warning(
        "OpenTelemetry was not configured: %s",
        telemetry_error,
    )

    tracer = None
    Status = None
    StatusCode = None


app = func.FunctionApp()


# ---------------------------------------------------------
# Helper: safely convert pandas/numpy values
# ---------------------------------------------------------

def safe_float(value: Any) -> float | None:
    """Convert a value to float, returning None for missing values."""

    if pd.isna(value):
        return None

    return float(value)


def safe_int(value: Any) -> int | None:
    """Convert a value to int, returning None for missing values."""

    if pd.isna(value):
        return None

    return int(value)


# ---------------------------------------------------------
# Helper: upload DataFrame to Azure Blob Storage
# ---------------------------------------------------------

def upload_csv_to_blob(
    dataframe: pd.DataFrame,
    container_name: str,
    blob_name: str,
) -> None:
    """Convert a DataFrame to CSV and upload it to Blob Storage."""

    storage_connection_string = os.getenv(
        "STORAGE_CONNECTION_STRING"
    )

    if not storage_connection_string:
        raise ValueError(
            "STORAGE_CONNECTION_STRING is missing."
        )

    csv_buffer = io.StringIO()

    dataframe.to_csv(
        csv_buffer,
        index=False,
    )

    blob_service_client = BlobServiceClient.from_connection_string(
        storage_connection_string
    )

    container_client = blob_service_client.get_container_client(
        container_name
    )

    try:
        container_client.create_container()

        logging.info(
            "Created Blob Storage container '%s'.",
            container_name,
        )

    except Exception:
        # The container probably already exists.
        pass

    blob_client = container_client.get_blob_client(
        blob_name
    )

    blob_client.upload_blob(
        csv_buffer.getvalue(),
        overwrite=True,
    )

    logging.info(
        "Uploaded '%s' successfully to Azure Blob Storage.",
        blob_name,
    )


# ---------------------------------------------------------
# Helper: send anomaly events to Azure Event Hubs
# ---------------------------------------------------------

def send_events_to_event_hub(
    anomalies: pd.DataFrame,
    symbol: str,
    execution_time: str,
) -> None:
    """Send anomaly summary and anomaly details to Event Hubs."""

    connection_string = os.getenv(
        "EVENT_HUB_SEND_CONNECTION_STRING"
    )

    event_hub_name = os.getenv(
        "EVENT_HUB_NAME"
    )

    if not connection_string:
        raise ValueError(
            "EVENT_HUB_CONNECTION_STRING is missing."
        )

    if not event_hub_name:
        raise ValueError(
            "EVENT_HUB_NAME is missing."
        )

    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=event_hub_name,
    )

    try:
        event_batch = producer.create_batch()

        summary_event = {
            "eventType": "StockAnomalyDetectionCompleted",
            "symbol": symbol,
            "executionTimeUtc": execution_time,
            "anomalyCount": int(len(anomalies)),
            "status": (
                "Alert"
                if len(anomalies) > 0
                else "Normal"
            ),
        }

        event_batch.add(
            EventData(
                json.dumps(summary_event)
            )
        )

        for _, row in anomalies.iterrows():
            anomaly_event = {
                "eventType": "StockAnomalyDetected",
                "symbol": symbol,
                "executionTimeUtc": execution_time,
                "date": str(
                    row.get("Date", "")
                ),
                "open": safe_float(
                    row.get("Open")
                ),
                "high": safe_float(
                    row.get("High")
                ),
                "low": safe_float(
                    row.get("Low")
                ),
                "close": safe_float(
                    row.get("Close")
                ),
                "volume": safe_int(
                    row.get("Volume")
                ),
                "anomaly": safe_int(
                    row.get("Anomaly")
                ),
            }

            event_batch.add(
                EventData(
                    json.dumps(anomaly_event)
                )
            )

        producer.send_batch(event_batch)

        logging.info(
            "Successfully sent %s anomaly event(s) "
            "to Event Hub '%s'.",
            len(anomalies),
            event_hub_name,
        )

    finally:
        producer.close()


# ---------------------------------------------------------
# Function 1: Timer-triggered anomaly detection
# ---------------------------------------------------------

@app.timer_trigger(
    schedule="0 0 9 * * *",
    arg_name="mytimer",
    run_on_startup=True,
    use_monitor=True,
)
def stock_anomaly_timer(
    mytimer: func.TimerRequest,
) -> None:
    """
    Download MSFT data, detect anomalies, upload CSV files,
    send events to Event Hubs, and record telemetry.
    """

    symbol = "MSFT"

    current_time = datetime.datetime.now(
        datetime.timezone.utc
    )

    main_span = None

    if tracer:
        main_span = tracer.start_span(
            "StockAnomalyDetection"
        )

    try:
        if main_span:
            main_span.set_attribute(
                "stock.symbol",
                symbol,
            )

            main_span.set_attribute(
                "function.trigger",
                "timer",
            )

            main_span.set_attribute(
                "function.execution_time",
                current_time.isoformat(),
            )

            main_span.set_attribute(
                "function.timer_past_due",
                bool(mytimer.past_due),
            )

        logging.info(
            "Stock anomaly detection started at %s.",
            current_time.isoformat(),
        )

        if mytimer.past_due:
            logging.warning(
                "The timer trigger is running later "
                "than scheduled."
            )

        # -------------------------------------------------
        # Step 1: Download Microsoft stock data
        # -------------------------------------------------

        stock = yf.download(
            tickers=symbol,
            period="1y",
            interval="1d",
            auto_adjust=False,
            progress=False,
        )

        if stock.empty:
            raise ValueError(
                "No Microsoft stock data was downloaded."
            )

        # yfinance may return MultiIndex columns.
        if isinstance(
            stock.columns,
            pd.MultiIndex,
        ):
            stock.columns = [
                column[0]
                if isinstance(column, tuple)
                else column
                for column in stock.columns
            ]

        if "Close" not in stock.columns:
            raise ValueError(
                "Downloaded stock data does not "
                "contain a Close column."
            )

        stock = stock.reset_index()

        logging.info(
            "Downloaded %s stock records for %s.",
            len(stock),
            symbol,
        )

        if main_span:
            main_span.set_attribute(
                "stock.downloaded_records",
                len(stock),
            )

        # -------------------------------------------------
        # Step 2: Prepare values for Isolation Forest
        # -------------------------------------------------

        valid_close_prices = (
            stock[["Close"]]
            .dropna()
        )

        if len(valid_close_prices) < 10:
            raise ValueError(
                "Not enough valid stock records "
                "for anomaly detection."
            )

        # -------------------------------------------------
        # Step 3: Detect anomalies
        # -------------------------------------------------

        model = IsolationForest(
            contamination=0.02,
            random_state=42,
        )

        predictions = model.fit_predict(
            valid_close_prices
        )

        stock["Anomaly"] = 1

        stock.loc[
            valid_close_prices.index,
            "Anomaly",
        ] = predictions

        stock["Anomaly"] = (
            stock["Anomaly"]
            .fillna(1)
            .astype(int)
        )

        anomalies = stock[
            stock["Anomaly"] == -1
        ].copy()

        anomaly_count = int(
            len(anomalies)
        )

        if anomaly_count > 0:
            logging.warning(
                "STOCK_ANOMALY_ALERT: %s anomalies "
                "detected for %s.",
                anomaly_count,
                symbol,
            )

        else:
            logging.info(
                "No anomalies detected for %s.",
                symbol,
            )

        if main_span:
            main_span.set_attribute(
                "stock.anomaly_count",
                anomaly_count,
            )

        # -------------------------------------------------
        # Step 4: Upload all stock data to Blob Storage
        # -------------------------------------------------

        upload_csv_to_blob(
            dataframe=stock,
            container_name="stock-data",
            blob_name="msft_stock_data.csv",
        )

        # -------------------------------------------------
        # Step 5: Upload anomaly-only data
        # -------------------------------------------------

        upload_csv_to_blob(
            dataframe=anomalies,
            container_name="stock-data",
            blob_name="msft_anomaly_results.csv",
        )

        # -------------------------------------------------
        # Step 6: Send anomaly events to Event Hubs
        # -------------------------------------------------

        send_events_to_event_hub(
            anomalies=anomalies,
            symbol=symbol,
            execution_time=current_time.isoformat(),
        )

        logging.info(
            "Stock anomaly detection completed. "
            "Total anomalies: %s.",
            anomaly_count,
        )

        if (
            main_span
            and Status
            and StatusCode
        ):
            main_span.set_status(
                Status(
                    StatusCode.OK
                )
            )

    except Exception as error:
        logging.exception(
            "Stock anomaly detection failed: %s",
            error,
        )

        if (
            main_span
            and Status
            and StatusCode
        ):
            main_span.record_exception(error)

            main_span.set_status(
                Status(
                    StatusCode.ERROR,
                    str(error),
                )
            )

        raise

    finally:
        if main_span:
            main_span.end()


# ---------------------------------------------------------
# Function 2: Event Hub consumer
# ---------------------------------------------------------

@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="stock-anomaly-events",
    connection="EVENT_HUB_LISTEN_CONNECTION_STRING",
    consumer_group="anomaly-alert-consumer",
)
def process_anomaly_event(
    event: func.EventHubEvent,
) -> None:
    """Process a message received from Azure Event Hubs."""

    try:
        raw_message = event.get_body().decode("utf-8")

        logging.info(
            "Received Event Hub message: %s",
            raw_message,
        )

        if not raw_message.strip():
            logging.warning(
                "Received an empty Event Hub message. Skipping."
            )
            return

        try:
            event_data = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            logging.error(
                "Invalid JSON received from Event Hub. Message=%r Error=%s",
                raw_message,
                exc,
            )
            return
        logging.info(
            "Successfully processed anomaly for %s",
            event_data.get("symbol")
        )

        event_type = event_data.get(
            "eventType"
        )


        



        symbol = event_data.get(
            "symbol",
            "Unknown",
        )

        if (
            event_type
            == "StockAnomalyDetectionCompleted"
        ):
            anomaly_count = int(
                event_data.get(
                    "anomalyCount",
                    0,
                )
            )

            status = event_data.get(
                "status",
                "Unknown",
            )

            logging.info(
                "Detection summary received. "
                "Symbol=%s, anomalyCount=%s, status=%s",
                symbol,
                anomaly_count,
                status,
            )

            if anomaly_count > 0:
                logging.warning(
                    "ANOMALY ALERT RECEIVED: "
                    "%s anomalies detected for %s.",
                    anomaly_count,
                    symbol,
                )

        elif (
            event_type
            == "StockAnomalyDetected"
        ):
            logging.warning(
                "Individual anomaly received. "
                "Symbol=%s, Date=%s, Close=%s",
                symbol,
                event_data.get("date"),
                event_data.get("close"),
            )

        else:
            logging.warning(
                "Unknown Event Hub event type: %s",
                event_type,
            )

    except json.JSONDecodeError:
        logging.exception(
            "The Event Hub message was not valid JSON."
        )

        raise

    except Exception as error:
        logging.exception(
            "Failed to process Event Hub message: %s",
            error,
        )

        raise