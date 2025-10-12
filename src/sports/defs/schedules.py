from typing import Union

import dagster as dg


@dg.schedule(cron_schedule="0 * * * *", target="*")
def sports_schedule(context: dg.ScheduleEvaluationContext):
    return dg.SkipReason(
        "Skipping. Change this to return a RunRequest to launch a run."
    )
