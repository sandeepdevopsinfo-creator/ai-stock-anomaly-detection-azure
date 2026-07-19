import datetime
import logging

import azure.functions as func

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 0 9 * * *",
    arg_name="mytimer",
    run_on_startup=True,
    use_monitor=False,
)
def stock_anomaly_timer(mytimer: func.TimerRequest) -> None:
    current_time = datetime.datetime.now(datetime.timezone.utc)

    logging.info(
        "Stock anomaly detection function started at %s",
        current_time.isoformat(),
    )