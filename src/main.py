from prefect import task, Flow
from prefect.schedules import IntervalSchedule
from datetime import timedelta, datetime
import boto3
import json
import os
from typing import Optional, Tuple, Dict, List

SQS_QUEUE_NAME = "doppel-dirk"
SNS_TOPIC = "doppel-dirk"
FLOW_NAME = "doppel-dirk-flow"


def _extract_file_bucket_and_path(s3_event_notification: Dict):
    s3_info = s3_event_notification["Records"][0]["s3"]
    return s3_info["bucket"]["name"], s3_info["object"]["key"]


def _is_new_file_event(event: Dict):
    return (
        event.get("Records", [{}])[0].get("eventName", "").startswith("ObjectCreated")
    )


@task(log_stdout=True)
def new_s3_files(queue_name: str) -> List[Tuple[str, str]]:
    print("checking for newly uploaded file events")

    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    messages = queue.receive_messages(
        MessageAttributeNames=["All"], MaxNumberOfMessages=10
    )
    events = [json.loads(message.body) for message in messages]
    bucket_path_tuples = [
        _extract_file_bucket_and_path(event)
        for event in events
        if _is_new_file_event(event)
    ]

    for message in messages:
        # we can safely delete all because only s3 events are expected on the queue
        message.delete()

    print(f"found {len(bucket_path_tuples)} new files")

    return bucket_path_tuples


def _account_id():
    sts = boto3.client("sts")
    return sts.get_caller_identity().get("Account")


def _publish_success(bucket: str, path: str):
    print("publishing success")
    client = boto3.client("sns")
    message = f"successfully processed {bucket}/{path}"
    client.publish(
        TargetArn=f"arn:aws:sns:eu-central-1:{_account_id()}:doppel-dirk",
        Message=json.dumps({"default": json.dumps(message)}),
        MessageStructure="json",
    )


@task(log_stdout=True)
def transform_and_load(bucket_and_path: Tuple[str, str]):
    print("transform_and_load")
    bucket, path = bucket_and_path
    s3 = boto3.client("s3")

    print(f"downloading {bucket}/{path}")
    local_path = f"./tmp/{path}"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "wb") as data:
        s3.download_fileobj(bucket, path, data)

    print(f"doing some fancy transformation")
    # do something fancy here

    print(f"uploading fancy result")
    result = "_SUCCESS"
    output_path = f"/processed/{path}.foo"
    s3 = boto3.resource("s3")
    s3.Object(bucket, output_path).put(Body=result)

    _publish_success(bucket, output_path)


def main():
    schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1),
        interval=timedelta(minutes=1),
    )
    with Flow(FLOW_NAME, schedule=schedule) as flow:
        bucket_path_tuples = new_s3_files(SQS_QUEUE_NAME)

        transform_and_load.map(bucket_path_tuples)

    flow.run()


if __name__ == "__main__":
    main()
