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
from openai import OpenAI
from sklearn.ensemble import IsolationForest


# -------------------------------------------------------------------
# Optional Azure Monitor / OpenTelemetry configuration
# -------------------------------------------------------------------

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


# -------------------------------------------------------------------
# Helper: generate Azure OpenAI explanation
# -------------------------------------------------------------------

def generate_ai_explanation(anomaly_data: dict[str, Any]) -> str:
    """Generate a professional explanation for a detected stock anomaly."""

    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT")

    missing_settings = []

    if not endpoint:
        missing_settings.append("AZURE_OPENAI_ENDPOINT")

    if not api_key:
        missing_settings.append("AZURE_OPENAI_API_KEY")

    if not deployment_name:
        missing_settings.append("AZURE_OPENAI_DEPLOYMENT")

    if missing_settings:
        logging.error(
            "Missing Azure OpenAI settings: %s",
            ", ".join(missing_settings),
        )

        return (
            "AI explanation is unavailable because the "
            "Azure OpenAI configuration is incomplete."
        )

    # AZURE_OPENAI_ENDPOINT must look like:
    # https://YOUR-RESOURCE-NAME.openai.azure.com
    if "/api/projects/" in endpoint:
        logging.error(
            "AZURE_OPENAI_ENDPOINT contains a Foundry project endpoint. "
            "Use the Azure OpenAI endpoint ending in openai.azure.com."
        )

        return (
            "AI explanation is unavailable because the configured endpoint "
            "is a Foundry project endpoint instead of an Azure OpenAI endpoint."
        )

    base_url = endpoint.rstrip("/")

    if not base_url.endswith("/openai/v1"):
        base_url = f"{base_url}/openai/v1"

    base_url = f"{base_url}/"

    prompt = f"""
You are an Azure AI stock-monitoring assistant.

Analyze the following detected stock anomaly:

{json.dumps(anomaly_data, indent=2)}

Return the following:

1. Short summary
2. Possible reasons
3. Risk level: Low, Medium, or High
4. Recommended investigation step

Keep the response under 150 words.
Do not provide financial advice.
"""

    try:
        ai_client = OpenAI(
            base_url=base_url,
            api_key=api_key,
        )

        response = ai_client.responses.create(
            model=deployment_name,
            input=prompt,
        )

        explanation = response.output_text.strip()

        logging.info(
            "AI explanation generated successfully: %s",
            explanation,
        )

        return explanation

    except Exception as error:
        logging.exception(
            "AI explanation generation failed: %s",
            error,
        )

        return "AI explanation could not be generated."


# -------------------------------------------------------------------
# Helpers: safely convert pandas/numpy values
# -------------------------------------------------------------------

def safe_float(value: Any) -> float | None:
    """Convert a value to float and return None for missing values."""

    if value is None or pd.isna(value):
        return None

    return float(value)


def safe_int(value: Any) -> int | None:
    """Convert a value to int and return None for missing values."""

    if value is None or pd.isna(value):
        return None

    return int(value)


# -------------------------------------------------------------------
# Helper: upload a DataFrame to Azure Blob Storage
# -------------------------------------------------------------------

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
            "Created Blob Storage container: %s",
            container_name,
        )

    except Exception:
        # The container likely already exists.
        pass

    blob_client = container_client.get_blob_client(
        blob_name
    )

    blob_client.upload_blob(
        csv_buffer.getvalue(),
        overwrite=True,
    )

    logging.info(
        "Uploaded %s successfully to Azure Blob Storage.",
        blob_name,
    )


# -------------------------------------------------------------------
# Helper: send anomaly events to Azure Event Hubs
# -------------------------------------------------------------------

def send_events_to_event_hub(
    anomalies: pd.DataFrame,
    symbol: str,
    execution_time: str,
) -> None:
    """Send an anomaly summary and anomaly records to Event Hubs."""

    connection_string = os.getenv(
        "EVENT_HUB_SEND_CONNECTION_STRING"
    )

    event_hub_name = os.getenv(
        "EVENT_HUB_NAME"
    )

    if not connection_string:
        raise ValueError(
            "EVENT_HUB_SEND_CONNECTION_STRING is missing."
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

        anomaly_count = int(len(anomalies))

        summary_event = {
            "eventType": "StockAnomalyDetectionCompleted",
            "symbol": symbol,
            "executionTimeUtc": execution_time,
            "anomalyCount": anomaly_count,
            "status": (
                "Alert"
                if anomaly_count > 0
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
                "date": str(row.get("Date", "")),
                "open": safe_float(row.get("Open")),
                "high": safe_float(row.get("High")),
                "low": safe_float(row.get("Low")),
                "close": safe_float(row.get("Close")),
                "volume": safe_int(row.get("Volume")),
                "anomaly": safe_int(row.get("Anomaly")),
            }

            event_batch.add(
                EventData(
                    json.dumps(anomaly_event)
                )
            )

        producer.send_batch(event_batch)

        logging.info(
            "Successfully sent %s anomaly event(s) to Event Hub %s.",
            anomaly_count,
            event_hub_name,
        )

    finally:
        producer.close()


# -------------------------------------------------------------------
# Function 1: timer-triggered stock anomaly detection
# -------------------------------------------------------------------

@app.timer_trigger(
    schedule="0 0 9 * * *",
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def stock_anomaly_timer(
    mytimer: func.TimerRequest,
) -> None:
    """
    Download Microsoft stock data, detect anomalies, upload CSV files,
    send Event Hub events, and generate an AI explanation.
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
                "The timer trigger is running later than scheduled."
            )

        # -----------------------------------------------------------
        # Step 1: Download stock data
        # -----------------------------------------------------------

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

        # yfinance can return MultiIndex columns.
        if isinstance(stock.columns, pd.MultiIndex):
            stock.columns = [
                column[0]
                if isinstance(column, tuple)
                else column
                for column in stock.columns
            ]

        if "Close" not in stock.columns:
            raise ValueError(
                "Downloaded stock data does not contain a Close column."
            )

        stock = stock.reset_index()

        # -----------------------------------------------------------
        # Step 2: Prepare model data
        # -----------------------------------------------------------

        required_columns = [
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
        ]

        available_columns = [
            column
            for column in required_columns
            if column in stock.columns
        ]

        if not available_columns:
            raise ValueError(
                "No numeric stock columns are available for detection."
            )

        model_data = stock[
            available_columns
        ].copy()

        model_data = model_data.ffill().bfill()

        if model_data.empty:
            raise ValueError(
                "Stock data is empty after preprocessing."
            )

        # -----------------------------------------------------------
        # Step 3: Detect anomalies
        # -----------------------------------------------------------

        isolation_forest = IsolationForest(
            n_estimators=200,
            contamination=0.05,
            random_state=42,
        )

        stock["Anomaly"] = isolation_forest.fit_predict(
            model_data
        )

        anomalies = stock[
            stock["Anomaly"] == -1
        ].copy()

        anomaly_count = int(len(anomalies))

        logging.info(
            "Detected %s stock anomalies.",
            anomaly_count,
        )

        if main_span:
            main_span.set_attribute(
                "stock.anomaly_count",
                anomaly_count,
            )

        # -----------------------------------------------------------
        # Step 4: Upload stock and anomaly CSV files
        # -----------------------------------------------------------

        upload_csv_to_blob(
            dataframe=stock,
            container_name="stock-data",
            blob_name="msft_stock_data.csv",
        )

        upload_csv_to_blob(
            dataframe=anomalies,
            container_name="stock-data",
            blob_name="msft_anomaly_results.csv",
        )

        # -----------------------------------------------------------
        # Step 5: Send Event Hub events
        # -----------------------------------------------------------

        send_events_to_event_hub(
            anomalies=anomalies,
            symbol=symbol,
            execution_time=current_time.isoformat(),
        )

        # -----------------------------------------------------------
        # Step 6: Generate an AI explanation
        # -----------------------------------------------------------

        if anomaly_count > 0:
            first_anomaly = anomalies.iloc[0]

            anomaly_data = {
                "symbol": symbol,
                "date": str(
                    first_anomaly.get("Date", "")
                ),
                "open": safe_float(
                    first_anomaly.get("Open")
                ),
                "high": safe_float(
                    first_anomaly.get("High")
                ),
                "low": safe_float(
                    first_anomaly.get("Low")
                ),
                "close": safe_float(
                    first_anomaly.get("Close")
                ),
                "volume": safe_int(
                    first_anomaly.get("Volume")
                ),
            }

            ai_explanation = generate_ai_explanation(
                anomaly_data
            )

            logging.info(
                "AI Explanation: %s",
                ai_explanation,
            )

        else:
            logging.info(
                "No AI explanation requested because no anomalies were found."
            )

        logging.info(
            "Stock anomaly detection completed. Total anomalies: %s.",
            anomaly_count,
        )

        if main_span and Status and StatusCode:
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

        if main_span and Status and StatusCode:
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


# -------------------------------------------------------------------
# Function 2: Event Hub anomaly-event processor
# -------------------------------------------------------------------

@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="%EVENT_HUB_NAME%",
    connection="EVENT_HUB_LISTEN_CONNECTION_STRING",
    consumer_group="anomaly-alert-consumer",
)
def process_anomaly_event(
    event: func.EventHubEvent,
) -> None:
    """Read and log stock-anomaly events received from Event Hubs."""

    try:
        event_body = event.get_body().decode(
            "utf-8"
        )

        event_data = json.loads(
            event_body
        )

        event_type = event_data.get(
            "eventType",
            "Unknown",
        )

        logging.info(
            "Received Event Hub message: %s",
            event_body,
        )

        if event_type == "StockAnomalyDetected":
            logging.warning(
                "Individual anomaly received for %s on %s. Close: %s",
                event_data.get("symbol"),
                event_data.get("date"),
                event_data.get("close"),
            )

        elif event_type == "StockAnomalyDetectionCompleted":
            logging.info(
                "Detection summary received. Symbol: %s, "
                "anomaly count: %s, status: %s",
                event_data.get("symbol"),
                event_data.get("anomalyCount"),
                event_data.get("status"),
            )

        else:
            logging.info(
                "Unhandled Event Hub event type: %s",
                event_type,
            )

    except json.JSONDecodeError as error:
        logging.exception(
            "Event Hub message was not valid JSON: %s",
            error,
        )

        raise

    except Exception as error:
        logging.exception(
            "Failed to process Event Hub message: %s",
            error,
        )

        raise